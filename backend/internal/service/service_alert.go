package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/repository"
)

type AlertService struct {
	recipients    *repository.RecipientRepository
	subscriptions *repository.SubscriptionRepository
	claims        *repository.DispatchClaimRepository
	sms           *SMSService
	timeout       time.Duration
	logger        *slog.Logger
}

func NewAlertService(recipients *repository.RecipientRepository, subscriptions *repository.SubscriptionRepository, claims *repository.DispatchClaimRepository, sms *SMSService, timeout time.Duration, logger *slog.Logger) *AlertService {
	return &AlertService{recipients: recipients, subscriptions: subscriptions, claims: claims, sms: sms, timeout: timeout, logger: logger}
}

// SendOverdueAlerts 扫描超期未确认对象并向订阅家属告警。告警按最近确认形成唯一事件，
// 同一对象在同一事件中对同一家属只有一条成功记录；扫描期间完成确认或对象被暂停时，
// 本次告警放弃并释放占位，不占用事件；发送失败只记失败，后续扫描可重试。
func (s *AlertService) SendOverdueAlerts(ctx context.Context) (DispatchSummary, error) {
	var summary DispatchSummary
	now := time.Now().UTC()
	items, err := s.recipients.Overdue(ctx, now.Add(-s.timeout))
	if err != nil {
		return summary, err
	}
	for i := range items {
		recipient := &items[i]
		anchor := AlertAnchor(recipient)
		eventKey := AlertEventKey(anchor)
		subs, err := s.subscriptions.ListActiveByRecipient(ctx, recipient.ID)
		if err != nil {
			summary.Failed++
			s.logger.Error("list subscriptions", "recipient_id", recipient.ID, "error", err)
			continue
		}
		for _, sub := range subs {
			claim, acquired, err := s.claims.TryAcquire(ctx, recipient.ID, sub.ID, constants.SMSKindAlert, eventKey, now)
			if err != nil {
				summary.Failed++
				s.logger.Error("acquire alert claim", "recipient_id", recipient.ID, "subscription_id", sub.ID, "event_key", eventKey, "error", err)
				continue
			}
			if !acquired {
				summary.Skipped++
				continue
			}
			sent, err := s.sendAlert(ctx, recipient.ID, anchor, eventKey, sub.ID, sub.FamilyPhone, claim.ID)
			switch {
			case errors.Is(err, errAlertAborted):
				summary.Released++
			case err != nil:
				summary.Failed++
				s.logger.Error("send alert", "recipient_id", recipient.ID, "subscription_id", sub.ID, "event_key", eventKey, "error", err)
			case sent:
				summary.Sent++
			}
		}
	}
	s.logger.Info("overdue alerts dispatched", "sent", summary.Sent, "skipped", summary.Skipped, "failed", summary.Failed, "released", summary.Released)
	return summary, nil
}

// errAlertAborted 表示本次告警在发送前被放弃（占位已释放，不占事件）。
var errAlertAborted = errors.New("alert aborted before send")

// sendAlert 在占位成功后复查对象状态：若扫描期间完成确认、对象被暂停或删除，
// 则释放占位放弃本次告警；否则发送并按结果流转占位状态。
func (s *AlertService) sendAlert(ctx context.Context, recipientID uint, anchor time.Time, eventKey string, subscriptionID uint, familyPhone string, claimID uint) (bool, error) {
	fresh, err := s.recipients.Get(ctx, recipientID)
	switch {
	case errors.Is(err, repository.ErrNotFound):
		s.releaseClaim(ctx, claimID, "recipient deleted during scan")
		return false, errAlertAborted
	case err != nil:
		s.failClaim(ctx, claimID, err)
		return false, fmt.Errorf("reload recipient %d: %w", recipientID, err)
	}
	if fresh.Status != constants.RecipientStatusActive {
		s.releaseClaim(ctx, claimID, "recipient paused during scan")
		return false, errAlertAborted
	}
	if fresh.LastConfirmedAt != nil && fresh.LastConfirmedAt.After(anchor) {
		s.releaseClaim(ctx, claimID, "confirmed during scan")
		return false, errAlertAborted
	}
	rid, sid := recipientID, subscriptionID
	content := fmt.Sprintf("关怀提醒：%s 已超过确认时限未回复，请尽快联系确认。", fresh.Name)
	logItem, err := s.sms.Send(ctx, SMSMessage{
		CareRecipientID:      &rid,
		FamilySubscriptionID: &sid,
		RecipientPhone:       familyPhone,
		Content:              content,
		Kind:                 constants.SMSKindAlert,
		PeriodKey:            eventKey,
		ClaimID:              &claimID,
	})
	if err != nil {
		s.failClaim(ctx, claimID, err)
		return false, err
	}
	if err := s.claims.MarkSent(ctx, claimID, logItem.ID); err != nil {
		return false, err
	}
	s.logger.Info("alert dispatched", "recipient_id", recipientID, "subscription_id", subscriptionID, "event_key", eventKey, "claim_id", claimID)
	return true, nil
}

func (s *AlertService) releaseClaim(ctx context.Context, claimID uint, reason string) {
	if err := s.claims.MarkReleased(ctx, claimID, reason); err != nil {
		s.logger.Warn("release alert claim", "claim_id", claimID, "error", err)
	}
}

func (s *AlertService) failClaim(ctx context.Context, claimID uint, cause error) {
	if err := s.claims.MarkFailed(ctx, claimID, cause.Error()); err != nil {
		s.logger.Warn("mark alert claim failed", "claim_id", claimID, "error", err)
	}
}
