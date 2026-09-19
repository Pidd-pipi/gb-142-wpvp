package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newAlertTestEnv(t *testing.T) (*gorm.DB, *AlertService, *stubSMSProvider) {
	t.Helper()
	return newAlertTestEnvWithDSN(t, ":memory:")
}

func newAlertTestEnvWithDSN(t *testing.T, dsn string) (*gorm.DB, *AlertService, *stubSMSProvider) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := model.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	provider := &stubSMSProvider{}
	sms := NewSMSService(provider, repository.NewSMSLogRepository(db), logger)
	svc := NewAlertService(
		repository.NewRecipientRepository(db),
		repository.NewSubscriptionRepository(db),
		repository.NewDispatchClaimRepository(db),
		sms, time.Hour, logger)
	return db, svc, provider
}

func createOverdueRecipient(t *testing.T, db *gorm.DB, name, phone string, lastConfirmedAt *time.Time) model.CareRecipient {
	t.Helper()
	recipient := model.CareRecipient{
		Name:            name,
		Phone:           phone,
		CareFrequency:   constants.FrequencyDaily,
		CareStartAt:     time.Now().UTC().Add(-72 * time.Hour),
		Status:          constants.RecipientStatusActive,
		LastConfirmedAt: lastConfirmedAt,
	}
	if err := db.Create(&recipient).Error; err != nil {
		t.Fatal(err)
	}
	return recipient
}

func subscribeFamily(t *testing.T, db *gorm.DB, recipientID uint, phone string) model.FamilySubscription {
	t.Helper()
	sub := model.FamilySubscription{CareRecipientID: recipientID, FamilyName: "家属", FamilyPhone: phone, Active: true}
	if err := db.Create(&sub).Error; err != nil {
		t.Fatal(err)
	}
	return sub
}

func TestAlertOncePerEventAndNewEventAfterConfirm(t *testing.T) {
	db, svc, _ := newAlertTestEnv(t)
	ctx := context.Background()
	confirmedAt := time.Now().UTC().Add(-48 * time.Hour)
	recipient := createOverdueRecipient(t, db, "王阿姨", "13800138000", &confirmedAt)
	sub := subscribeFamily(t, db, recipient.ID, "13900139000")

	summary, err := svc.SendOverdueAlerts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Sent != 1 {
		t.Fatalf("first scan summary=%+v, want sent=1", summary)
	}
	// 同一事件重复扫描不得重复告警。
	summary, err = svc.SendOverdueAlerts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Sent != 0 || summary.Skipped != 1 {
		t.Fatalf("repeat scan summary=%+v, want skipped=1 sent=0", summary)
	}
	if got := countSMSLogs(t, db, recipient.ID, constants.SMSKindAlert, constants.SMSResultSuccess); got != 1 {
		t.Fatalf("alert success logs=%d, want 1", got)
	}

	// 新的确认形成新事件：再次超期后允许重新告警。
	staleConfirm := time.Now().UTC().Add(-2 * time.Hour)
	if err := db.Model(&model.CareRecipient{}).Where("id = ?", recipient.ID).Update("last_confirmed_at", staleConfirm).Error; err != nil {
		t.Fatal(err)
	}
	summary, err = svc.SendOverdueAlerts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Sent != 1 {
		t.Fatalf("new event scan summary=%+v, want sent=1", summary)
	}
	if got := countSMSLogs(t, db, recipient.ID, constants.SMSKindAlert, constants.SMSResultSuccess); got != 2 {
		t.Fatalf("alert success logs=%d, want 2 (one per event)", got)
	}
	var claims []model.DispatchClaim
	if err := db.Where("care_recipient_id = ? AND family_subscription_id = ? AND kind = ?", recipient.ID, sub.ID, constants.SMSKindAlert).Find(&claims).Error; err != nil {
		t.Fatal(err)
	}
	if len(claims) != 2 || claims[0].PeriodKey == claims[1].PeriodKey {
		t.Fatalf("claims=%+v, want 2 distinct events", claims)
	}
}

func TestAlertAbortedWhenConfirmedDuringScan(t *testing.T) {
	db, svc, provider := newAlertTestEnv(t)
	ctx := context.Background()
	recipient := createOverdueRecipient(t, db, "李大爷", "13800138001", nil)
	sub := subscribeFamily(t, db, recipient.ID, "13900139001")

	// 模拟扫描快照后、发送前完成确认：锚点取快照时刻的关怀开始时间。
	anchor := AlertAnchor(&recipient)
	eventKey := AlertEventKey(anchor)
	claimRepo := repository.NewDispatchClaimRepository(db)
	claim, acquired, err := claimRepo.TryAcquire(ctx, recipient.ID, sub.ID, constants.SMSKindAlert, eventKey, time.Now().UTC())
	if err != nil || !acquired {
		t.Fatalf("acquire claim: acquired=%v err=%v", acquired, err)
	}
	confirmedAt := time.Now().UTC()
	if err := db.Model(&model.CareRecipient{}).Where("id = ?", recipient.ID).Update("last_confirmed_at", confirmedAt).Error; err != nil {
		t.Fatal(err)
	}

	sent, err := svc.sendAlert(ctx, recipient.ID, anchor, eventKey, sub.ID, sub.FamilyPhone, claim.ID)
	if !errors.Is(err, errAlertAborted) || sent {
		t.Fatalf("sendAlert: sent=%v err=%v, want aborted", sent, err)
	}
	if provider.callCount() != 0 {
		t.Fatalf("provider calls=%d, want 0 (alert aborted)", provider.callCount())
	}
	if got := countSMSLogs(t, db, recipient.ID, constants.SMSKindAlert, ""); got != 0 {
		t.Fatalf("alert logs=%d, want 0", got)
	}
	var stored model.DispatchClaim
	if err := db.First(&stored, claim.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.State != constants.ClaimStateReleased {
		t.Fatalf("claim state=%q, want released", stored.State)
	}
	// 放弃不占事件：同一事件可再次被认领。
	if _, acquired, err := claimRepo.TryAcquire(ctx, recipient.ID, sub.ID, constants.SMSKindAlert, eventKey, time.Now().UTC()); err != nil || !acquired {
		t.Fatalf("reacquire released event: acquired=%v err=%v", acquired, err)
	}
}

func TestAlertFailureRetryable(t *testing.T) {
	db, svc, provider := newAlertTestEnv(t)
	ctx := context.Background()
	recipient := createOverdueRecipient(t, db, "赵奶奶", "13800138002", nil)
	subscribeFamily(t, db, recipient.ID, "13900139002")

	provider.setFail(true)
	summary, err := svc.SendOverdueAlerts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Sent != 0 || summary.Failed != 1 {
		t.Fatalf("failing scan summary=%+v, want failed=1", summary)
	}
	if got := countSMSLogs(t, db, recipient.ID, constants.SMSKindAlert, constants.SMSResultFailed); got != 1 {
		t.Fatalf("failed logs=%d, want 1", got)
	}

	provider.setFail(false)
	summary, err = svc.SendOverdueAlerts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Sent != 1 {
		t.Fatalf("retry scan summary=%+v, want sent=1", summary)
	}
	if got := countSMSLogs(t, db, recipient.ID, constants.SMSKindAlert, constants.SMSResultSuccess); got != 1 {
		t.Fatalf("success logs=%d, want 1", got)
	}
}

func TestAlertConcurrentScansSendOnce(t *testing.T) {
	// 文件库配合 busy_timeout 支持多连接并发写，模拟并发扫描与重复触发。
	db, svc, provider := newAlertTestEnvWithDSN(t, "file:"+t.TempDir()+"/alert.db?_busy_timeout=5000&_journal_mode=WAL")
	ctx := context.Background()
	recipient := createOverdueRecipient(t, db, "孙爷爷", "13800138003", nil)
	subscribeFamily(t, db, recipient.ID, "13900139003")

	const scans = 8
	summaries := make(chan DispatchSummary, scans)
	errs := make(chan error, scans)
	for i := 0; i < scans; i++ {
		go func() {
			summary, err := svc.SendOverdueAlerts(ctx)
			if err != nil {
				errs <- err
				return
			}
			summaries <- summary
		}()
	}
	totalSent := 0
	for i := 0; i < scans; i++ {
		select {
		case err := <-errs:
			t.Fatalf("concurrent scan error: %v", err)
		case summary := <-summaries:
			totalSent += summary.Sent
		}
	}
	if totalSent != 1 {
		t.Fatalf("concurrent scans sent=%d, want exactly 1", totalSent)
	}
	if provider.callCount() != 1 {
		t.Fatalf("provider calls=%d, want 1", provider.callCount())
	}
	if got := countSMSLogs(t, db, recipient.ID, constants.SMSKindAlert, constants.SMSResultSuccess); got != 1 {
		t.Fatalf("alert success logs=%d, want 1", got)
	}
}
