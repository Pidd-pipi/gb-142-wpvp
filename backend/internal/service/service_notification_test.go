package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// stubSMSProvider 可控制成败的发送桩，用于模拟发送失败与恢复。
type stubSMSProvider struct {
	mu    sync.Mutex
	fail  bool
	calls int
}

func (p *stubSMSProvider) Send(_ context.Context, _ SMSMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.fail {
		return errors.New("provider outage")
	}
	return nil
}

func (p *stubSMSProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *stubSMSProvider) setFail(fail bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fail = fail
}

func newNotificationTestEnv(t *testing.T) (*gorm.DB, *NotificationService, *stubSMSProvider) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := model.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	provider := &stubSMSProvider{}
	sms := NewSMSService(provider, repository.NewSMSLogRepository(db), logger)
	svc := NewNotificationService(
		repository.NewRecipientRepository(db),
		repository.NewTemplateRepository(db),
		repository.NewDispatchClaimRepository(db),
		sms, logger)
	return db, svc, provider
}

func countSMSLogs(t *testing.T, db *gorm.DB, recipientID uint, kind, result string) int64 {
	t.Helper()
	var total int64
	q := db.Model(&model.SMSLog{}).Where("care_recipient_id = ? AND kind = ?", recipientID, kind)
	if result != "" {
		q = q.Where("result = ?", result)
	}
	if err := q.Count(&total).Error; err != nil {
		t.Fatal(err)
	}
	return total
}

func TestGreetingIdempotentAcrossScans(t *testing.T) {
	db, svc, provider := newNotificationTestEnv(t)
	ctx := context.Background()
	now := time.Now().UTC()
	recipient := model.CareRecipient{Name: "王阿姨", Phone: "13800138000", CareFrequency: constants.FrequencyDaily, CareStartAt: now.Add(-time.Hour), Status: constants.RecipientStatusActive}
	if err := db.Create(&recipient).Error; err != nil {
		t.Fatal(err)
	}
	template := model.SMSTemplate{Category: "general", Content: "{{name}}，早上好", Active: true}
	if err := db.Create(&template).Error; err != nil {
		t.Fatal(err)
	}

	summary, err := svc.SendDueGreetings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Sent != 1 || summary.Skipped != 0 || summary.Failed != 0 {
		t.Fatalf("first scan summary=%+v, want sent=1", summary)
	}
	if got := countSMSLogs(t, db, recipient.ID, constants.SMSKindGreeting, constants.SMSResultSuccess); got != 1 {
		t.Fatalf("success logs=%d, want 1", got)
	}

	// 模拟 LastGreetingAt 未生效的重复触发：到期预筛被绕过，占位仍须挡住重发。
	if err := db.Model(&model.CareRecipient{}).Where("id = ?", recipient.ID).Update("last_greeting_at", nil).Error; err != nil {
		t.Fatal(err)
	}
	summary, err = svc.SendDueGreetings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Sent != 0 || summary.Skipped != 1 {
		t.Fatalf("repeat scan summary=%+v, want skipped=1 sent=0", summary)
	}
	if got := countSMSLogs(t, db, recipient.ID, constants.SMSKindGreeting, ""); got != 1 {
		t.Fatalf("total logs=%d, want 1 (no duplicate send)", got)
	}
	if provider.callCount() != 1 {
		t.Fatalf("provider calls=%d, want 1", provider.callCount())
	}
}

func TestGreetingFailureRetryable(t *testing.T) {
	db, svc, provider := newNotificationTestEnv(t)
	ctx := context.Background()
	now := time.Now().UTC()
	recipient := model.CareRecipient{Name: "李大爷", Phone: "13800138001", CareFrequency: constants.FrequencyDaily, CareStartAt: now.Add(-time.Hour), Status: constants.RecipientStatusActive}
	if err := db.Create(&recipient).Error; err != nil {
		t.Fatal(err)
	}

	provider.setFail(true)
	summary, err := svc.SendDueGreetings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Sent != 0 || summary.Failed != 1 {
		t.Fatalf("failing scan summary=%+v, want failed=1", summary)
	}
	if got := countSMSLogs(t, db, recipient.ID, constants.SMSKindGreeting, constants.SMSResultFailed); got != 1 {
		t.Fatalf("failed logs=%d, want 1", got)
	}
	var claim model.DispatchClaim
	if err := db.Where("care_recipient_id = ? AND kind = ?", recipient.ID, constants.SMSKindGreeting).First(&claim).Error; err != nil {
		t.Fatal(err)
	}
	if claim.State != constants.ClaimStateFailed {
		t.Fatalf("claim state=%q, want failed", claim.State)
	}

	// 发送恢复后，后续扫描可重试并成功，且同周期只有一条成功记录。
	provider.setFail(false)
	summary, err = svc.SendDueGreetings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Sent != 1 {
		t.Fatalf("retry scan summary=%+v, want sent=1", summary)
	}
	if got := countSMSLogs(t, db, recipient.ID, constants.SMSKindGreeting, constants.SMSResultSuccess); got != 1 {
		t.Fatalf("success logs=%d, want 1", got)
	}
	if err := db.Where("care_recipient_id = ? AND kind = ?", recipient.ID, constants.SMSKindGreeting).First(&claim).Error; err != nil {
		t.Fatal(err)
	}
	if claim.State != constants.ClaimStateSent || claim.SMSLogID == nil {
		t.Fatalf("claim=%+v, want sent with log", claim)
	}
}
