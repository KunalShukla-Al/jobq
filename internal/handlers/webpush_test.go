package handlers_test

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/SherClockHolmes/webpush-go"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/KunalShukla-Al/jobq/internal/handlers"
	"github.com/KunalShukla-Al/jobq/internal/queue"
	"github.com/KunalShukla-Al/jobq/internal/testdb"
	"github.com/KunalShukla-Al/jobq/internal/worker"
)

var ctx = context.Background()

// acadkitTables mirrors the two tables from AcadKit's
// supabase/migrations/011_push.sql that the webpush handler uses, with
// push_subscriptions.vapid_public from 041_push_vapid_key.sql.
const acadkitTables = `
create table if not exists public.push_subscriptions (
  id uuid primary key default gen_random_uuid(),
  device_id text not null,
  endpoint text not null unique,
  p256dh text not null,
  auth text not null,
  ua text,
  created_at timestamptz default now()
);
alter table public.push_subscriptions add column if not exists vapid_public text;
create index if not exists idx_push_device on public.push_subscriptions(device_id);

create table if not exists public.sent_notifications (
  id uuid primary key default gen_random_uuid(),
  device_id text not null,
  kind text not null,
  ref text not null,
  sent_at timestamptz default now(),
  unique (device_id, kind, ref)
);`

// browser is one subscribed browser: the keys a real one keeps, so the fake
// push service can decrypt what it's sent.
type browser struct {
	key  *ecdh.PrivateKey
	auth []byte
}

func newBrowser(t *testing.T) browser {
	t.Helper()
	key, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	auth := make([]byte, 16)
	_, _ = rand.Read(auth)
	return browser{key, auth}
}

// decrypt undoes RFC 8291 (aes128gcm) for this browser.
func (b browser) decrypt(body []byte) ([]byte, error) {
	if len(body) < 21 || len(body) < 21+int(body[20]) {
		return nil, errors.New("short body")
	}
	salt, idLen := body[:16], int(body[20])
	serverPub, err := ecdh.P256().NewPublicKey(body[21 : 21+idLen])
	if err != nil {
		return nil, err
	}
	shared, err := b.key.ECDH(serverPub)
	if err != nil {
		return nil, err
	}
	info := append(append([]byte("WebPush: info\x00"), b.key.PublicKey().Bytes()...), serverPub.Bytes()...)
	ikm, err := hkdf.Key(sha256.New, shared, b.auth, string(info), 32)
	if err != nil {
		return nil, err
	}
	cek, _ := hkdf.Key(sha256.New, ikm, salt, "Content-Encoding: aes128gcm\x00", 16)
	nonce, _ := hkdf.Key(sha256.New, ikm, salt, "Content-Encoding: nonce\x00", 12)
	block, _ := aes.NewCipher(cek)
	gcm, _ := cipher.NewGCM(block)
	plain, err := gcm.Open(nil, nonce, body[21+idLen:], nil)
	if err != nil {
		return nil, err
	}
	plain = bytes.TrimRight(plain, "\x00") // padding, then the 0x02 delimiter
	return bytes.TrimSuffix(plain, []byte{2}), nil
}

// fakePush is a push service. An endpoint's first path segment is the
// status it answers with: /201/..., /410/..., /500/...
type fakePush struct {
	*httptest.Server
	mu       sync.Mutex
	browsers map[string]browser // by path
	got      map[string][]byte  // decrypted message, by path
	headers  map[string]http.Header
}

func newFakePush(t *testing.T) *fakePush {
	f := &fakePush{browsers: map[string]browser{}, got: map[string][]byte{}, headers: map[string]http.Header{}}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		defer f.mu.Unlock()
		msg, err := f.browsers[r.URL.Path].decrypt(body)
		if err != nil {
			t.Errorf("%s: can't decrypt: %v", r.URL.Path, err)
		}
		f.got[r.URL.Path], f.headers[r.URL.Path] = msg, r.Header
		status, _ := strconv.Atoi(strings.Split(r.URL.Path, "/")[1])
		w.WriteHeader(status)
	}))
	t.Cleanup(f.Close)
	return f
}

// subscribe adds a browser for device to AcadKit's table, at an endpoint
// that answers status, subscribed with the VAPID public key vapid ("" for
// AcadKit's original key, stored as null), and returns the endpoint.
func (f *fakePush) subscribe(t *testing.T, db *pgxpool.Pool, device string, status int, vapid string) string {
	t.Helper()
	b := newBrowser(t)
	path := fmt.Sprintf("/%d/%s/%d", status, device, len(f.browsers))
	f.mu.Lock()
	f.browsers[path] = b
	f.mu.Unlock()
	enc := base64.RawURLEncoding.EncodeToString
	if _, err := db.Exec(ctx, `insert into public.push_subscriptions (device_id, endpoint, p256dh, auth, vapid_public) values ($1, $2, $3, $4, nullif($5, ''))`,
		device, f.URL+path, enc(b.key.PublicKey().Bytes()), enc(b.auth), vapid); err != nil {
		t.Fatal(err)
	}
	return f.URL + path
}

func (f *fakePush) received(endpoint string) ([]byte, http.Header) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := strings.TrimPrefix(endpoint, f.URL)
	return f.got[path], f.headers[path]
}

// forget clears what was received, so a later run's pushes can be told apart.
func (f *fakePush) forget() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got, f.headers = map[string][]byte{}, map[string]http.Header{}
}

func setup(t *testing.T) (*pgxpool.Pool, *handlers.WebPush, string) {
	t.Helper()
	t.Parallel()
	db := testdb.New(t)
	if _, err := db.Exec(ctx, acadkitTables); err != nil {
		t.Fatal(err)
	}
	private, public, err := webpush.GenerateVAPIDKeys()
	if err != nil {
		t.Fatal(err)
	}
	h, err := handlers.NewWebPush(db, public, private, "mailto:test@example.com")
	if err != nil {
		t.Fatal(err)
	}
	return db, h, public
}

func job(t *testing.T, p handlers.WebPushPayload) queue.Job {
	t.Helper()
	body, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return queue.Job{ID: 1, Kind: "webpush", Payload: body, Attempts: 1, MaxAttempts: 8}
}

func count(t *testing.T, db *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestWebPush(t *testing.T) {
	db, h, vapidPublic := setup(t)
	push := newFakePush(t)
	url := "/attendance"

	for _, c := range []struct {
		name       string
		statuses   []int  // one subscription per status
		want       string // "ok", "retry" or "permanent"
		recorded   bool   // marked in sent_notifications
		subsAfter  int
		deliveries int // subscriptions that got the message
	}{
		{"delivered to every browser, and recorded", []int{201, 201}, "ok", true, 2, 2},
		{"gone (410): subscription deleted, not recorded", []int{410}, "ok", false, 0, 1},
		{"one gone (404), one delivered: recorded", []int{404, 201}, "ok", true, 1, 2},
		{"push service error (500): retried, not recorded", []int{500}, "retry", false, 1, 1},
		{"one delivered, one 500: retried, not recorded", []int{201, 500}, "retry", false, 2, 2},
		{"rate limited (429): retried", []int{429}, "retry", false, 1, 1},
		{"rejected (403): permanent", []int{403}, "permanent", false, 1, 1},
		{"one rejected (400), one delivered: recorded", []int{400, 201}, "ok", true, 2, 2},
		{"no subscriptions: nothing to do, not recorded", nil, "ok", false, 0, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			device := strings.NewReplacer(" ", "-", "(", "", ")", "", ":", "", ",", "").Replace(c.name)
			var endpoints []string
			for _, s := range c.statuses {
				endpoints = append(endpoints, push.subscribe(t, db, device, s, vapidPublic))
			}
			err := h.Handle(ctx, job(t, handlers.WebPushPayload{
				DeviceID: device, Kind: "class", Ref: "slot-1|2026-11-03",
				Title: "DBMS in 10 min", Body: "Room <TP 401> & lab after", URL: &url,
			}))

			if got := outcome(err); got != c.want {
				t.Fatalf("outcome %s (err %v), want %s", got, err, c.want)
			}
			if got := count(t, db, `select count(*) from public.sent_notifications where device_id = $1 and kind = 'class' and ref = 'slot-1|2026-11-03'`, device); (got == 1) != c.recorded {
				t.Errorf("sent_notifications rows: %d, want recorded=%v", got, c.recorded)
			}
			if got := count(t, db, `select count(*) from public.push_subscriptions where device_id = $1`, device); got != c.subsAfter {
				t.Errorf("subscriptions left: %d, want %d", got, c.subsAfter)
			}
			deliveries := 0
			for _, e := range endpoints {
				msg, hdr := push.received(e)
				if msg == nil {
					continue
				}
				deliveries++
				// Exactly what the Edge Function sends today: JSON.stringify's
				// output, so "<", ">" and "&" are not escaped.
				if want := `{"title":"DBMS in 10 min","body":"Room <TP 401> & lab after","url":"/attendance","tag":"class"}`; string(msg) != want {
					t.Errorf("message:\n got %s\nwant %s", msg, want)
				}
				if hdr.Get("TTL") != "43200" || hdr.Get("Content-Encoding") != "aes128gcm" {
					t.Errorf("headers: TTL=%q Content-Encoding=%q", hdr.Get("TTL"), hdr.Get("Content-Encoding"))
				}
				checkVAPID(t, hdr.Get("Authorization"), vapidPublic, push.URL)
			}
			if deliveries != c.deliveries {
				t.Errorf("%d browsers got the message, want %d", deliveries, c.deliveries)
			}
		})
	}
}

// A second run of a job that was delivered and marked (its lease ran out
// before it was recorded done) sends nothing and records it once.
func TestWebPushRetryThenRecordOnce(t *testing.T) {
	db, h, public := setup(t)
	push := newFakePush(t)
	endpoint := push.subscribe(t, db, "d", 201, public)
	j := job(t, handlers.WebPushPayload{DeviceID: "d", Kind: "deadline", Ref: "r", Title: "t", Body: "b"})
	if err := h.Handle(ctx, j); err != nil {
		t.Fatal(err)
	}
	if msg, _ := push.received(endpoint); string(msg) != `{"title":"t","body":"b","url":null,"tag":"deadline"}` {
		t.Fatalf("message %s: a missing url is sent as null, like today", msg)
	}
	push.forget()
	if err := h.Handle(ctx, j); err != nil {
		t.Fatal(err)
	}
	if msg, _ := push.received(endpoint); msg != nil {
		t.Fatalf("the second run pushed %s again", msg)
	}
	if n := count(t, db, `select count(*) from public.sent_notifications`); n != 1 {
		t.Fatalf("%d sent_notifications rows, want 1", n)
	}
}

// Rolled back while a job waited for its retry: the Edge Function sent the
// reminder and marked it, so the job sends nothing when it runs.
func TestWebPushSkipsWhatTheEdgeFunctionSent(t *testing.T) {
	db, h, public := setup(t)
	push := newFakePush(t)
	endpoint := push.subscribe(t, db, "d", 201, public)
	if _, err := db.Exec(ctx, `insert into public.sent_notifications (device_id, kind, ref) values ('d', 'class', 'r')`); err != nil {
		t.Fatal(err)
	}
	if err := h.Handle(ctx, job(t, handlers.WebPushPayload{DeviceID: "d", Kind: "class", Ref: "r", Title: "t"})); err != nil {
		t.Fatal(err)
	}
	if msg, _ := push.received(endpoint); msg != nil {
		t.Fatalf("pushed %s for a reminder already sent", msg)
	}
}

func TestWebPushNetworkErrorRetries(t *testing.T) {
	db, h, public := setup(t)
	push := newFakePush(t)
	endpoint := push.subscribe(t, db, "d", 201, public)
	push.Close() // the push service is down
	err := h.Handle(ctx, job(t, handlers.WebPushPayload{DeviceID: "d", Kind: "k", Ref: "r", Title: "t"}))
	if outcome(err) != "retry" {
		t.Fatalf("outcome %s (err %v), want retry", outcome(err), err)
	}
	// The error is logged and stored: it names the host, not the endpoint's path.
	if path := strings.TrimPrefix(endpoint, push.URL); strings.Contains(err.Error(), path) {
		t.Fatalf("error %q shows the endpoint's path %s", err, path)
	}
}

// A stored endpoint that can't be pushed to fails for good, not on every
// retry: with another browser that got it, the reminder is done and recorded.
func TestWebPushUnusableEndpointIsPermanent(t *testing.T) {
	db, h, public := setup(t)
	push := newFakePush(t)
	b := newBrowser(t)
	enc := base64.RawURLEncoding.EncodeToString
	for i, endpoint := range []string{"not a url", "ftp://push.example/x", "https:///no-host", "%zz"} {
		device := fmt.Sprint("d", i)
		if _, err := db.Exec(ctx, `insert into public.push_subscriptions (device_id, endpoint, p256dh, auth, vapid_public) values ($1, $2, $3, $4, $5)`,
			device, endpoint, enc(b.key.PublicKey().Bytes()), enc(b.auth), public); err != nil {
			t.Fatal(err)
		}
		j := job(t, handlers.WebPushPayload{DeviceID: device, Kind: "class", Ref: "r", Title: "t"})
		if err := h.Handle(ctx, j); outcome(err) != "permanent" {
			t.Errorf("endpoint %q alone: outcome %s (err %v), want permanent", endpoint, outcome(err), err)
		}
		push.subscribe(t, db, device, 201, public)
		if err := h.Handle(ctx, j); outcome(err) != "ok" {
			t.Errorf("endpoint %q and a working one: outcome %s (err %v), want ok", endpoint, outcome(err), err)
		}
	}
}

// While AcadKit moves to a new VAPID key, a device can have subscriptions on
// the old key (vapid_public null), which jobq can't sign for, next to ones on
// jobq's key. Only the latter are pushed; the old row is left alone (the Edge
// Function still serves it). Its endpoint answers 410, so a push to it would
// also have deleted it.
func TestWebPushSkipsOtherKeys(t *testing.T) {
	db, h, public := setup(t)
	var logged bytes.Buffer
	h.Logger = slog.New(slog.NewTextHandler(&logged, nil))
	push := newFakePush(t)
	old := push.subscribe(t, db, "d", 410, "")
	ours := push.subscribe(t, db, "d", 201, public)
	before := subscriptionRow(t, db, old)

	if err := h.Handle(ctx, job(t, handlers.WebPushPayload{DeviceID: "d", Kind: "class", Ref: "r", Title: "t"})); err != nil {
		t.Fatalf("err %v, want done", err)
	}
	if msg, hdr := push.received(ours); msg == nil {
		t.Error("the subscription on our key got nothing")
	} else {
		checkVAPID(t, hdr.Get("Authorization"), public, push.URL)
	}
	if msg, _ := push.received(old); msg != nil {
		t.Errorf("the old-key subscription was pushed %s", msg)
	}
	if after := subscriptionRow(t, db, old); after != before {
		t.Errorf("old-key row changed:\n got %s\nwant %s", after, before)
	}
	if n := count(t, db, `select count(*) from public.sent_notifications where device_id = 'd'`); n != 1 {
		t.Errorf("%d sent_notifications rows, want 1", n)
	}
	if !strings.Contains(logged.String(), "skipped=1") {
		t.Errorf("log %q doesn't say one subscription was skipped", logged.String())
	}
}

// The key secret may be padded or end in a newline; subscriptions stored
// with the plain key are still ours.
func TestWebPushMatchesKeyAsStored(t *testing.T) {
	db, _, _ := setup(t)
	private, public, err := webpush.GenerateVAPIDKeys()
	if err != nil {
		t.Fatal(err)
	}
	for i, configured := range []string{public + "=", public + "\n"} {
		h, err := handlers.NewWebPush(db, configured, private, "mailto:test@example.com")
		if err != nil {
			t.Fatalf("%q: %v", configured, err)
		}
		push := newFakePush(t)
		device := fmt.Sprint("d", i)
		endpoint := push.subscribe(t, db, device, 201, public)
		if err := h.Handle(ctx, job(t, handlers.WebPushPayload{DeviceID: device, Kind: "class", Ref: "r", Title: "t"})); err != nil {
			t.Fatalf("%q: err %v, want done", configured, err)
		}
		if msg, hdr := push.received(endpoint); msg == nil {
			t.Errorf("key configured as %q: the subscription on it got nothing", configured)
		} else {
			checkVAPID(t, hdr.Get("Authorization"), public, push.URL)
		}
	}
}

// A device with no subscription on jobq's key is done, with nothing pushed,
// deleted or recorded: the Edge Function can still send it the reminder.
func TestWebPushNoSubscriptionOnOurKey(t *testing.T) {
	db, h, _ := setup(t)
	var logged bytes.Buffer
	h.Logger = slog.New(slog.NewTextHandler(&logged, nil))
	push := newFakePush(t)
	_, otherKey, err := webpush.GenerateVAPIDKeys()
	if err != nil {
		t.Fatal(err)
	}
	endpoints := []string{
		push.subscribe(t, db, "d", 410, ""),       // the original key
		push.subscribe(t, db, "d", 201, ""),       // the original key
		push.subscribe(t, db, "d", 410, otherKey), // some other key
	}

	if err := h.Handle(ctx, job(t, handlers.WebPushPayload{DeviceID: "d", Kind: "class", Ref: "r", Title: "t"})); err != nil {
		t.Fatalf("err %v, want done", err)
	}
	for _, e := range endpoints {
		if msg, _ := push.received(e); msg != nil {
			t.Errorf("pushed %s to a subscription on another key", msg)
		}
	}
	if n := count(t, db, `select count(*) from public.push_subscriptions where device_id = 'd'`); n != 3 {
		t.Errorf("%d subscriptions left, want all 3", n)
	}
	if n := count(t, db, `select count(*) from public.sent_notifications`); n != 0 {
		t.Errorf("%d sent_notifications rows, want none", n)
	}
	if !strings.Contains(logged.String(), "skipped=3") {
		t.Errorf("log %q doesn't say three subscriptions were skipped", logged.String())
	}
}

// subscriptionRow is the whole row for endpoint, as text.
func subscriptionRow(t *testing.T, db *pgxpool.Pool, endpoint string) string {
	t.Helper()
	var row string
	if err := db.QueryRow(ctx, `select s::text from public.push_subscriptions s where endpoint = $1`, endpoint).Scan(&row); err != nil {
		t.Fatal(err)
	}
	return row
}

func TestWebPushBadPayloadIsPermanent(t *testing.T) {
	_, h, _ := setup(t)
	for _, payload := range []string{`not json`, `{"device_id": 7}`, `{"kind":"class","ref":"r","title":"t"}`, `{}`} {
		err := h.Handle(ctx, queue.Job{Kind: "webpush", Payload: json.RawMessage(payload)})
		if outcome(err) != "permanent" {
			t.Errorf("payload %s: outcome %s (err %v), want permanent", payload, outcome(err), err)
		}
	}
}

func TestNewWebPushRejectsBadKeys(t *testing.T) {
	t.Parallel()
	private, public, _ := webpush.GenerateVAPIDKeys()
	otherPrivate, _, _ := webpush.GenerateVAPIDKeys()
	for _, keys := range [][2]string{{"", private}, {public, ""}, {"not base64!", private}, {public, public}, {private, private}, {public, otherPrivate}} {
		if _, err := handlers.NewWebPush(nil, keys[0], keys[1], ""); err == nil {
			t.Errorf("keys %.12q / %.12q accepted", keys[0], keys[1])
		}
	}
	if _, err := handlers.NewWebPush(nil, public, private, ""); err != nil {
		t.Errorf("good keys rejected: %v", err)
	}
}

// checkVAPID verifies the Authorization header: "vapid t=<JWT>, k=<public
// key>", the JWT signed by that key, for this push service, from our subject.
func checkVAPID(t *testing.T, header, public, audience string) {
	t.Helper()
	token, key, ok := strings.Cut(strings.TrimPrefix(header, "vapid t="), ", k=")
	if !ok || key != public {
		t.Errorf("Authorization %.40q: not vapid t=..., k=<our public key>", header)
		return
	}
	parts := strings.Split(token, ".")
	raw, _ := base64.RawURLEncoding.DecodeString(key)
	x, y := elliptic.Unmarshal(elliptic.P256(), raw) //nolint:staticcheck // the simplest way from bytes to an ecdsa key
	sig, _ := base64.RawURLEncoding.DecodeString(parts[len(parts)-1])
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if x == nil || len(sig) != 64 || !ecdsa.Verify(&ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}, digest[:],
		new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
		t.Errorf("VAPID JWT not signed by our key")
	}
	body, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var claims struct{ Aud, Sub string }
	_ = json.Unmarshal(body, &claims)
	if claims.Aud != audience || claims.Sub != "mailto:test@example.com" {
		t.Errorf("VAPID claims aud=%q sub=%q, want %q and mailto:test@example.com", claims.Aud, claims.Sub, audience)
	}
}

func outcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case worker.IsPermanent(err):
		return "permanent"
	default:
		return "retry"
	}
}
