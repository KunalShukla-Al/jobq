package handlers

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/SherClockHolmes/webpush-go"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/KunalShukla-Al/jobq/internal/queue"
	"github.com/KunalShukla-Al/jobq/internal/worker"
)

// WebPushPayload is one reminder for one device, as AcadKit's planner
// enqueues it (idempotency key "webpush:<device_id>:<kind>:<ref>").
type WebPushPayload struct {
	DeviceID string  `json:"device_id"`
	Kind     string  `json:"kind"` // class | deadline | mark | ...; also the notification's tag
	Ref      string  `json:"ref"`  // stable key for the thing reminded about
	Title    string  `json:"title"`
	Body     string  `json:"body"`
	URL      *string `json:"url"`
}

// WebPush delivers a reminder to every browser the device has subscribed,
// then marks it sent in public.sent_notifications: only after delivery, so a
// push that fails is retried instead of lost (DESIGN.md, problem 1).
//
// Subscriptions are read when the job runs, not when it's enqueued, so a
// browser that subscribed or unsubscribed in between is handled right.
//
// A reminder already in sent_notifications isn't sent again. A retry after a
// partial failure (not yet marked) sends to every subscription again,
// including any that already got it. That's accepted: the notification's tag
// is its kind, so on a device the second one replaces the first instead of
// showing twice.
//
// Only subscriptions made with this handler's VAPID public key are pushed
// (push_subscriptions.vapid_public, AcadKit migration 041): a push service
// rejects a push signed by another key. The others, made with the key
// AcadKit used before (vapid_public null) and still served by the Edge
// Function, are skipped: not sent to, not failures, not deleted. A device
// whose subscriptions are all on another key is done with nothing recorded.
type WebPush struct {
	DB         *pgxpool.Pool
	public     string
	private    string
	subscriber string
	// Client sends the requests (an http.Client with Timeout per push).
	Client webpush.HTTPClient
	// TTL is how long the push service keeps a message for a device that's
	// offline. A reminder delivered half a day late is noise.
	TTL time.Duration
	// Logger notes subscriptions skipped for being on another key.
	Logger *slog.Logger
}

// NewWebPush makes the handler from the VAPID key pair (base64url, as
// web-push generates them: AcadKit's new pair, the Edge Function's
// VAPID_PUBLIC_V2 / VAPID_PRIVATE_V2) and
// subject (a mailto: or https: URL). Bad keys are an error here, not a
// failure of every job later.
func NewWebPush(db *pgxpool.Pool, vapidPublic, vapidPrivate, subject string) (*WebPush, error) {
	pub, err := decodeKey(vapidPublic)
	if err != nil {
		return nil, fmt.Errorf("VAPID public key: %w", err)
	}
	if _, err := ecdh.P256().NewPublicKey(pub); err != nil {
		return nil, fmt.Errorf("VAPID public key: %w", err)
	}
	priv, err := decodeKey(vapidPrivate)
	if err != nil {
		return nil, fmt.Errorf("VAPID private key: %w", err)
	}
	key, err := ecdh.P256().NewPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("VAPID private key: %w", err)
	}
	// A push service rejects a push whose key doesn't sign it, and only
	// subscriptions on the public key are pushed at all: with a mismatched
	// pair every reminder would fail, or be skipped without a word.
	if !bytes.Equal(key.PublicKey().Bytes(), pub) {
		return nil, errors.New("VAPID private key doesn't belong to the public key")
	}
	// Compared with push_subscriptions.vapid_public, which AcadKit stores
	// unpadded with no whitespace: a secret with "=" or a newline would
	// otherwise match no subscription.
	vapidPublic = base64.RawURLEncoding.EncodeToString(pub)
	if subject == "" {
		subject = "mailto:acadkit@example.com" // the Edge Function's fallback
	}
	// webpush-go adds "mailto:" to anything that isn't an https: URL, so a
	// subject that already has it would become "mailto:mailto:...".
	subject = strings.TrimPrefix(subject, "mailto:")
	return &WebPush{
		DB: db, public: vapidPublic, private: vapidPrivate, subscriber: subject,
		Client: &http.Client{Timeout: 10 * time.Second},
		TTL:    12 * time.Hour,
		Logger: slog.Default(),
	}, nil
}

func decodeKey(k string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(k, "="))
}

type subscription struct{ endpoint, p256dh, auth string }

// Handle sends one reminder. Per subscription: 200/201/202 is delivered;
// 404/410 means the browser unsubscribed, so the row is deleted and it
// doesn't count against the job; 429, 5xx and network errors are worth
// retrying; 400/401/403/413 aren't.
func (w *WebPush) Handle(ctx context.Context, j queue.Job) error {
	var p WebPushPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return worker.Permanent(fmt.Errorf("bad webpush payload: %w", err))
	}
	if p.DeviceID == "" || p.Kind == "" || p.Ref == "" || p.Title == "" {
		return worker.Permanent(errors.New("bad webpush payload: device_id, kind, ref and title are required"))
	}
	msg, err := message(p)
	if err != nil {
		return worker.Permanent(err)
	}

	// Already sent: by an earlier run of this job that marked it but lost its
	// lease before the job was recorded done, or by the Edge Function after a
	// rollback while this job waited for a retry. Sending again would repeat it.
	var sent bool
	if err := w.DB.QueryRow(ctx, `
		select exists (select 1 from public.sent_notifications
		               where device_id = $1 and kind = $2 and ref = $3)`,
		p.DeviceID, p.Kind, p.Ref).Scan(&sent); err != nil {
		return err
	}
	if sent {
		return nil
	}

	subs, skipped, err := w.subscriptions(ctx, p.DeviceID)
	if err != nil {
		return err
	}
	if skipped > 0 {
		// The job's id, not the device's: this log can be public.
		w.Logger.Info("webpush: skipped subscriptions on another VAPID key",
			"job", j.ID, "skipped", skipped, "on_our_key", len(subs))
	}
	if len(subs) == 0 {
		return nil // nothing to deliver to; not marked sent, since nothing was
	}

	var delivered int
	var retryable, permanent []error
	for _, s := range subs {
		switch err := w.send(ctx, msg, s); {
		case err == nil:
			delivered++
		case errors.Is(err, errGone):
			if _, err := w.DB.Exec(ctx, `delete from public.push_subscriptions where endpoint = $1`, s.endpoint); err != nil {
				retryable = append(retryable, fmt.Errorf("deleting a gone subscription: %w", err))
			}
		case worker.IsPermanent(err):
			permanent = append(permanent, err)
		default:
			retryable = append(retryable, err)
		}
	}

	switch {
	case len(retryable) > 0:
		return fmt.Errorf("%d of %d pushes delivered: %w", delivered, len(subs), errors.Join(retryable...))
	case delivered == 0 && len(permanent) > 0:
		return worker.Permanent(fmt.Errorf("no push delivered: %w", errors.Join(permanent...)))
	case delivered == 0:
		return nil // every subscription was gone, and is now deleted
	}
	_, err = w.DB.Exec(ctx, `
		insert into public.sent_notifications (device_id, kind, ref) values ($1, $2, $3)
		on conflict (device_id, kind, ref) do nothing`, p.DeviceID, p.Kind, p.Ref)
	if err != nil {
		return fmt.Errorf("delivered, but marking it sent failed: %w", err)
	}
	return nil
}

// message is what the service worker receives: the same JSON the Edge
// Function sends today, byte for byte (no HTML escaping, like JSON.stringify).
func message(p WebPushPayload) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	err := enc.Encode(struct {
		Title string  `json:"title"`
		Body  string  `json:"body"`
		URL   *string `json:"url"`
		Tag   string  `json:"tag"`
	}{p.Title, p.Body, p.URL, p.Kind})
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), err
}

// subscriptions returns the device's subscriptions made with our public key,
// and how many others it has (on another key, or the old one: null).
func (w *WebPush) subscriptions(ctx context.Context, device string) ([]subscription, int, error) {
	rows, err := w.DB.Query(ctx, `
		select endpoint, p256dh, auth, vapid_public is not distinct from $2
		from public.push_subscriptions
		where device_id = $1 order by created_at, endpoint`, device, w.public)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var subs []subscription
	skipped := 0
	for rows.Next() {
		var s subscription
		var ours bool
		if err := rows.Scan(&s.endpoint, &s.p256dh, &s.auth, &ours); err != nil {
			return nil, 0, err
		}
		if !ours {
			skipped++
			continue
		}
		subs = append(subs, s)
	}
	return subs, skipped, rows.Err()
}

var errGone = errors.New("subscription gone")

// send pushes msg to one subscription. It returns nil when delivered,
// errGone, a permanent error, or a retryable one.
func (w *WebPush) send(ctx context.Context, msg []byte, s subscription) error {
	// An endpoint that isn't an http(s) URL fails the same way every time, and
	// as a *url.Error, which below would mean "retry": each retry would then
	// push the reminder again to the device's working browsers.
	if u, err := url.Parse(s.endpoint); err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return worker.Permanent(fmt.Errorf("push to %s: the endpoint isn't an http(s) URL", host(s.endpoint)))
	}
	resp, err := webpush.SendNotificationWithContext(ctx, msg,
		&webpush.Subscription{Endpoint: s.endpoint, Keys: webpush.Keys{P256dh: s.p256dh, Auth: s.auth}},
		&webpush.Options{
			HTTPClient:      w.Client,
			Subscriber:      w.subscriber,
			TTL:             int(w.TTL / time.Second),
			VAPIDPublicKey:  w.public,
			VAPIDPrivateKey: w.private,
		})
	if err != nil {
		var netErr *url.Error // the request itself failed: timeout, refused, DNS...
		if errors.As(err, &netErr) {
			// Its text quotes the whole endpoint, secret path included, and
			// errors end up in last_error and in the (public) Actions log.
			return fmt.Errorf("push to %s: %w", host(s.endpoint), netErr.Err)
		}
		// Encrypting failed before anything was sent: the stored keys are
		// unusable, and they will be next time too.
		return worker.Permanent(fmt.Errorf("push to %s: %w", host(s.endpoint), err))
	}
	defer resp.Body.Close()
	detail, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted:
		return nil
	case http.StatusNotFound, http.StatusGone:
		return errGone
	}
	err = fmt.Errorf("push to %s: %s %s", host(s.endpoint), resp.Status, strings.TrimSpace(string(detail)))
	switch resp.StatusCode {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusRequestEntityTooLarge:
		return worker.Permanent(err)
	}
	return err // 429, 5xx, and anything unexpected: try again later
}

// host names the push service in errors without the endpoint's secret path.
func host(endpoint string) string {
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
		return u.Host
	}
	return "the push service"
}
