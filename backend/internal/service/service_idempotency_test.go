package service

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/dto"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// fakeProvider counts delivery attempts and can be configured to fail a given
// number of initial calls. It sleeps briefly on success so concurrent scans
// overlap and genuinely contend for the same lease.
type fakeProvider struct {
	mu       sync.Mutex
	calls    int32
	failNext int32
	delay    time.Duration
}

func (f *fakeProvider) Send(_ context.Context, msg SMSMessage) error {
	atomic.AddInt32(&f.calls, 1)
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	if remaining := atomic.AddInt32(&f.failNext, -1); remaining >= 0 {
		return fmt.Errorf("simulated provider outage")
	}
	return nil
}

func (f *fakeProvider) totalCalls() int { return int(atomic.LoadInt32(&f.calls)) }

type harness struct {
	db         *gorm.DB
	recipients *repository.RecipientRepository
	subs       *repository.SubscriptionRepository
	templates  *repository.TemplateRepository
	logs       *repository.SMSLogRepository
	confirms   *repository.ReplyConfirmRepository
	occupancy  *repository.SendOccupancyRepository
	provider   *fakeProvider
	sms        *SMSService
	notify     *NotificationService
	alerts     *AlertService
	recipientS *RecipientService
	logger     *slog.Logger
}

func newHarness(t *testing.T, timeout time.Duration) *harness {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", sanitize(t.Name()))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := model.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := &harness{
		db:         db,
		recipients: repository.NewRecipientRepository(db),
		subs:       repository.NewSubscriptionRepository(db),
		templates:  repository.NewTemplateRepository(db),
		logs:       repository.NewSMSLogRepository(db),
		confirms:   repository.NewReplyConfirmRepository(db),
		occupancy:  repository.NewSendOccupancyRepository(db),
		provider:   &fakeProvider{delay: 5 * time.Millisecond},
		logger:     logger,
	}
	h.sms = NewSMSService(h.provider, h.logs)
	h.notify = NewNotificationService(h.recipients, h.templates, h.occupancy, h.sms, logger)
	h.alerts = NewAlertService(h.recipients, h.subs, h.occupancy, h.sms, timeout, logger)
	h.recipientS = NewRecipientService(h.recipients, h.confirms, logger)
	return h
}

func sanitize(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r == '/' || r == ' ' {
			r = '_'
		}
		out = append(out, r)
	}
	return string(out)
}

func (h *harness) mustRecipient(t *testing.T, name, phone, freq string, start time.Time) *model.CareRecipient {
	t.Helper()
	item := &model.CareRecipient{
		Name: name, Phone: phone, CareFrequency: freq, CareStartAt: start,
		Status: constants.RecipientStatusActive,
	}
	if err := h.recipients.Create(context.Background(), item); err != nil {
		t.Fatal(err)
	}
	return item
}

func (h *harness) successGreetingCount(t *testing.T, recipientID uint) int {
	t.Helper()
	items, _, err := h.logs.List(context.Background(), 1, 200, repository.SMSLogFilter{
		CareRecipientID: &recipientID, Kind: constants.SMSKindGreeting, Result: constants.SMSResultSuccess,
	})
	if err != nil {
		t.Fatal(err)
	}
	return len(items)
}

func TestGreetingConcurrentScansSendOncePerPeriod(t *testing.T) {
	h := newHarness(t, time.Hour)
	r := h.mustRecipient(t, "王阿姨", "13800138000", constants.FrequencyDaily, time.Now().UTC().Add(-2*time.Hour))

	const scans = 12
	var wg sync.WaitGroup
	var sent int64
	for i := 0; i < scans; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := h.notify.SendDueGreetings(context.Background())
			if err != nil {
				t.Errorf("scan: %v", err)
				return
			}
			atomic.AddInt64(&sent, int64(n))
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt64(&sent); got != 1 {
		t.Fatalf("concurrent delivered greetings=%d, want 1", got)
	}
	if h.provider.totalCalls() != 1 {
		t.Fatalf("provider delivery calls=%d, want 1", h.provider.totalCalls())
	}
	if c := h.successGreetingCount(t, r.ID); c != 1 {
		t.Fatalf("success greeting logs=%d, want 1", c)
	}

	// A duplicate manual trigger still cannot send again in the same period.
	if _, err := h.notify.SendDueGreetings(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.provider.totalCalls() != 1 {
		t.Fatalf("provider calls after re-trigger=%d, want 1", h.provider.totalCalls())
	}
}

func TestGreetingFailureOnlyRecordedThenRetrySucceeds(t *testing.T) {
	h := newHarness(t, time.Hour)
	r := h.mustRecipient(t, "李叔", "13800138001", constants.FrequencyDaily, time.Now().UTC().Add(-2*time.Hour))
	h.provider.failNext = 1

	n, err := h.notify.SendDueGreetings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("failed scan delivered=%d, want 0", n)
	}

	failed, _, err := h.logs.List(context.Background(), 1, 50, repository.SMSLogFilter{
		CareRecipientID: &r.ID, Kind: constants.SMSKindGreeting, Result: constants.SMSResultFailed,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 1 || failed[0].FailureReason == "" {
		t.Fatalf("expected one failed log with reason, got %+v", failed)
	}
	if c := h.successGreetingCount(t, r.ID); c != 0 {
		t.Fatalf("success logs=%d, want 0 after failure", c)
	}
	occ, _, err := h.occupancy.List(context.Background(), 1, 10, &r.ID, constants.SMSKindGreeting)
	if err != nil || len(occ) != 1 || occ[0].Status != constants.OccupancyFailed {
		t.Fatalf("occupancy after failure=%+v err=%v", occ, err)
	}

	// A later scan retries the still-open period and succeeds once.
	n, err = h.notify.SendDueGreetings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("retry delivered=%d, want 1", n)
	}
	if h.provider.totalCalls() != 2 {
		t.Fatalf("provider calls=%d, want 2 (one failed + one success)", h.provider.totalCalls())
	}
	if c := h.successGreetingCount(t, r.ID); c != 1 {
		t.Fatalf("success logs after retry=%d, want 1", c)
	}

	// Once retried successfully, the period is closed and never resends.
	if _, err := h.notify.SendDueGreetings(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.provider.totalCalls() != 2 {
		t.Fatalf("provider calls after close=%d, want 2", h.provider.totalCalls())
	}
}

func TestAlertOneSuccessPerConfirmationEvent(t *testing.T) {
	h := newHarness(t, time.Hour)
	// Never confirmed and started long ago -> overdue.
	r := h.mustRecipient(t, "张奶奶", "13800138002", constants.FrequencyDaily, time.Now().UTC().Add(-48*time.Hour))
	sub := model.FamilySubscription{CareRecipientID: r.ID, FamilyName: "子", FamilyPhone: "13900000001", Active: true}
	if err := h.subs.Create(context.Background(), &sub); err != nil {
		t.Fatal(err)
	}

	const scans = 10
	var wg sync.WaitGroup
	var sent int64
	for i := 0; i < scans; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := h.alerts.SendOverdueAlerts(context.Background())
			if err != nil {
				t.Errorf("alert scan: %v", err)
				return
			}
			atomic.AddInt64(&sent, int64(n))
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt64(&sent); got != 1 {
		t.Fatalf("concurrent alerts delivered=%d, want 1", got)
	}
	if h.provider.totalCalls() != 1 {
		t.Fatalf("provider calls=%d, want 1", h.provider.totalCalls())
	}

	// A new confirmation opens a brand-new event; once overdue again it alerts once more.
	if _, err := h.recipientS.Confirm(context.Background(), r.ID, dto.ConfirmRecipientRequest{Channel: "sms"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.alerts.SendOverdueAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.provider.totalCalls() != 1 {
		t.Fatalf("provider calls right after confirm=%d, want 1 (not overdue)", h.provider.totalCalls())
	}

	// Move confirmation far enough into the past for the same recipient to be overdue again.
	fresh, err := h.recipients.Get(context.Background(), r.ID)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-3 * time.Hour)
	fresh.LastConfirmedAt = &old
	if err := h.recipients.Update(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := h.alerts.SendOverdueAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.provider.totalCalls() != 2 {
		t.Fatalf("provider calls after new overdue event=%d, want 2", h.provider.totalCalls())
	}
	// Duplicate triggers within this new event must not resend.
	if _, err := h.alerts.SendOverdueAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.provider.totalCalls() != 2 {
		t.Fatalf("provider calls after duplicate trigger=%d, want 2", h.provider.totalCalls())
	}

	success, _, err := h.logs.List(context.Background(), 1, 50, repository.SMSLogFilter{
		CareRecipientID: &r.ID, Kind: constants.SMSKindAlert, Result: constants.SMSResultSuccess,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(success) != 2 {
		t.Fatalf("successful alert logs across two events=%d, want 2", len(success))
	}
	if success[0].EventKey == success[1].EventKey {
		t.Fatal("the two events must have distinct event keys")
	}
}

func TestAlertConfirmationDuringScanIsDroppedAndDoesNotOccupy(t *testing.T) {
	h := newHarness(t, time.Hour)
	r := h.mustRecipient(t, "赵大爷", "13800138003", constants.FrequencyDaily, time.Now().UTC().Add(-48*time.Hour))
	sub := model.FamilySubscription{CareRecipientID: r.ID, FamilyName: "女", FamilyPhone: "13900000002", Active: true}
	if err := h.subs.Create(context.Background(), &sub); err != nil {
		t.Fatal(err)
	}

	// Confirmation lands after the lease is acquired but before the recheck.
	h.alerts.beforeDispatch = func(_ context.Context, recipientID uint) {
		if _, err := h.recipientS.Confirm(context.Background(), recipientID, dto.ConfirmRecipientRequest{Channel: "manual"}); err != nil {
			t.Errorf("mid-scan confirm: %v", err)
		}
	}
	n, err := h.alerts.SendOverdueAlerts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("alerts delivered during confirm race=%d, want 0", n)
	}
	if h.provider.totalCalls() != 0 {
		t.Fatalf("provider calls during confirm race=%d, want 0", h.provider.totalCalls())
	}

	occ, _, err := h.occupancy.List(context.Background(), 1, 10, &r.ID, constants.SMSKindAlert)
	if err != nil || len(occ) != 1 || occ[0].Status != constants.OccupancyAbandoned {
		t.Fatalf("occupancy=%+v err=%v, want single abandoned", occ, err)
	}

	// Remove the hook and age the confirmation past timeout. Abandoned must not
	// block the genuinely overdue event that follows.
	h.alerts.beforeDispatch = nil
	fresh, err := h.recipients.Get(context.Background(), r.ID)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-3 * time.Hour)
	fresh.LastConfirmedAt = &old
	if err := h.recipients.Update(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := h.alerts.SendOverdueAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.provider.totalCalls() != 1 {
		t.Fatalf("provider calls after abandoned-then-overdue=%d, want 1", h.provider.totalCalls())
	}
}

func TestAlertFailureRecordedAndRetried(t *testing.T) {
	h := newHarness(t, time.Hour)
	r := h.mustRecipient(t, "钱婶", "13800138004", constants.FrequencyDaily, time.Now().UTC().Add(-48*time.Hour))
	sub := model.FamilySubscription{CareRecipientID: r.ID, FamilyName: "儿", FamilyPhone: "13900000003", Active: true}
	if err := h.subs.Create(context.Background(), &sub); err != nil {
		t.Fatal(err)
	}
	h.provider.failNext = 1

	n, err := h.alerts.SendOverdueAlerts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("failed alert delivered=%d, want 0", n)
	}
	failed, _, err := h.logs.List(context.Background(), 1, 50, repository.SMSLogFilter{
		CareRecipientID: &r.ID, Kind: constants.SMSKindAlert, Result: constants.SMSResultFailed,
	})
	if err != nil || len(failed) != 1 {
		t.Fatalf("failed alert logs=%+v err=%v", failed, err)
	}

	if _, err := h.alerts.SendOverdueAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.provider.totalCalls() != 2 {
		t.Fatalf("provider calls after retry=%d, want 2", h.provider.totalCalls())
	}
	success, _, err := h.logs.List(context.Background(), 1, 50, repository.SMSLogFilter{
		CareRecipientID: &r.ID, Kind: constants.SMSKindAlert, Result: constants.SMSResultSuccess,
	})
	if err != nil || len(success) != 1 {
		t.Fatalf("success alert logs=%+v err=%v", success, err)
	}

	// Event now closed.
	if _, err := h.alerts.SendOverdueAlerts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.provider.totalCalls() != 2 {
		t.Fatalf("provider calls after close=%d, want 2", h.provider.totalCalls())
	}
}

func TestAlertOneSubscriptionFailureDoesNotBlockAnother(t *testing.T) {
	h := newHarness(t, time.Hour)
	r := h.mustRecipient(t, "孙老", "13800138005", constants.FrequencyDaily, time.Now().UTC().Add(-48*time.Hour))
	subA := model.FamilySubscription{CareRecipientID: r.ID, FamilyName: "A", FamilyPhone: "13900000004", Active: true}
	subB := model.FamilySubscription{CareRecipientID: r.ID, FamilyName: "B", FamilyPhone: "13900000005", Active: true}
	if err := h.subs.Create(context.Background(), &subA); err != nil {
		t.Fatal(err)
	}
	if err := h.subs.Create(context.Background(), &subB); err != nil {
		t.Fatal(err)
	}
	// Fail only the first delivery (subscriptions are iterated in id order).
	h.provider.failNext = 1

	n, err := h.alerts.SendOverdueAlerts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("delivered=%d, want 1 (one failed, one succeeded in same scan)", n)
	}
	success, _, err := h.logs.List(context.Background(), 1, 50, repository.SMSLogFilter{
		CareRecipientID: &r.ID, Kind: constants.SMSKindAlert, Result: constants.SMSResultSuccess,
	})
	if err != nil || len(success) != 1 {
		t.Fatalf("success logs=%+v err=%v", success, err)
	}
}

func TestGreetingPeriodKeyStableWithinPeriod(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	r := &model.CareRecipient{CareFrequency: constants.FrequencyDaily, CareStartAt: start}
	k1 := GreetingPeriodKey(r, start.Add(2*time.Hour))
	k2 := GreetingPeriodKey(r, start.Add(23*time.Hour))
	if k1 != k2 {
		t.Fatalf("same daily period keys differ: %q vs %q", k1, k2)
	}
	k3 := GreetingPeriodKey(r, start.Add(25*time.Hour))
	if k1 == k3 {
		t.Fatal("next daily period must produce a new key")
	}
}
