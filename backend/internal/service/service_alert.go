package service

import (
	"context"
	"fmt"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
	"log/slog"
)

type AlertService struct {
	recipients    *repository.RecipientRepository
	subscriptions *repository.SubscriptionRepository
	occupancy     *repository.SendOccupancyRepository
	sms           *SMSService
	timeout       time.Duration
	logger        *slog.Logger

	// beforeDispatch is invoked after a lease is acquired and before the
	// post-claim recheck; production leaves it nil. Tests use it to emulate a
	// confirmation landing mid-scan.
	beforeDispatch func(ctx context.Context, recipientID uint)
}

func NewAlertService(recipients *repository.RecipientRepository, subscriptions *repository.SubscriptionRepository, occupancy *repository.SendOccupancyRepository, sms *SMSService, timeout time.Duration, logger *slog.Logger) *AlertService {
	return &AlertService{recipients: recipients, subscriptions: subscriptions, occupancy: occupancy, sms: sms, timeout: timeout, logger: logger}
}

// SendOverdueAlerts scans recipients past the confirmation timeout and alerts
// every active family subscription. The event key is derived from the latest
// confirmation, so all duplicate/concurrent/retry attempts within one event
// collapse into a single successful alert per subscription. Per-target
// failures never abort the whole scan.
func (s *AlertService) SendOverdueAlerts(ctx context.Context) (int, error) {
	now := time.Now().UTC()
	cutoff := now.Add(-s.timeout)
	items, err := s.recipients.Overdue(ctx, cutoff)
	if err != nil {
		return 0, err
	}
	sent := 0
	for _, recipient := range items {
		subs, err := s.subscriptions.ListActiveByRecipient(ctx, recipient.ID)
		if err != nil {
			s.logger.Error("list subscriptions for alert",
				"recipient_id", recipient.ID, "error", err)
			continue
		}
		for _, sub := range subs {
			delivered, err := s.dispatchAlert(ctx, &recipient, &sub, now, cutoff)
			if err != nil {
				s.logger.Error("alert dispatch failed",
					"recipient_id", recipient.ID, "subscription_id", sub.ID, "error", err)
				continue
			}
			if delivered {
				sent++
			}
		}
	}
	s.logger.Info("overdue alert scan finished", "candidates", len(items), "sent", sent)
	return sent, nil
}

func (s *AlertService) dispatchAlert(ctx context.Context, recipient *model.CareRecipient, sub *model.FamilySubscription, now, cutoff time.Time) (bool, error) {
	eventKey := AlertEventKey(recipient.LastConfirmedAt)
	claim, err := s.occupancy.Claim(ctx, constants.SMSKindAlert, recipient.ID, sub.ID, "", eventKey, occupancyLease, now)
	if err != nil {
		return false, err
	}
	if !claim.Acquired {
		s.logger.Info("alert event already closed or in flight",
			"recipient_id", recipient.ID, "subscription_id", sub.ID,
			"event_key", eventKey, "status", claim.Existing.Status)
		return false, nil
	}
	occ := claim.Occupancy

	if s.beforeDispatch != nil {
		s.beforeDispatch(ctx, recipient.ID)
	}

	// Post-claim recheck: a confirmation recorded between scan and send makes
	// the current event stale. Abandon without sending; abandoned does not
	// close anything, so the next genuinely overdue event remains alertable.
	fresh, err := s.recipients.Get(ctx, recipient.ID)
	if err != nil {
		_ = s.occupancy.MarkAbandoned(ctx, occ.ID, "recipient reload failed: "+err.Error(), time.Now().UTC())
		return false, err
	}
	if !isOverdueAt(fresh, cutoff) || AlertEventKey(fresh.LastConfirmedAt) != eventKey {
		if err := s.occupancy.MarkAbandoned(ctx, occ.ID, "recipient confirmed during scan; event dropped", time.Now().UTC()); err != nil {
			return false, err
		}
		s.logger.Info("alert abandoned, confirmation landed during scan",
			"recipient_id", recipient.ID, "event_key", eventKey)
		return false, nil
	}

	content := fmt.Sprintf("关怀提醒：%s 已超过确认时限未回复，请尽快联系确认。", fresh.Name)
	rid, sid, occID := fresh.ID, sub.ID, occ.ID
	logItem, sendErr := s.sms.Send(ctx, SMSMessage{
		CareRecipientID: &rid, FamilySubscriptionID: &sid, RecipientPhone: sub.FamilyPhone,
		Content: content, Kind: constants.SMSKindAlert, EventKey: eventKey, OccupancyID: &occID,
	})
	finishAt := time.Now().UTC()
	if sendErr != nil {
		// Failure only records a failure; the event stays open for a later retry.
		if err := s.occupancy.MarkFailure(ctx, occ.ID, sendErr.Error(), finishAt); err != nil {
			return false, err
		}
		return false, sendErr
	}
	if err := s.occupancy.MarkSuccess(ctx, occ.ID, logItem.ID, finishAt); err != nil {
		return false, err
	}
	s.logger.Info("overdue alert dispatched",
		"recipient_id", fresh.ID, "subscription_id", sub.ID,
		"event_key", eventKey, "sms_log_id", logItem.ID)
	return true, nil
}
