package service

import (
	"context"
	"strings"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
	"log/slog"
)

// occupancyLease bounds how long a processing ownership may be held before a
// crashed run is considered dead and another scan may take over.
const occupancyLease = 2 * time.Minute

type NotificationService struct {
	recipients *repository.RecipientRepository
	templates  *repository.TemplateRepository
	occupancy  *repository.SendOccupancyRepository
	sms        *SMSService
	logger     *slog.Logger
}

func NewNotificationService(recipients *repository.RecipientRepository, templates *repository.TemplateRepository, occupancy *repository.SendOccupancyRepository, sms *SMSService, logger *slog.Logger) *NotificationService {
	return &NotificationService{recipients: recipients, templates: templates, occupancy: occupancy, sms: sms, logger: logger}
}

// SendDueGreetings scans all recipients whose current care period is due.
// Concurrent scans, duplicate triggers and retries all funnel through the
// unique period occupancy, so each recipient gets at most one successful
// greeting per care period. Per-recipient failures never abort the whole scan.
func (s *NotificationService) SendDueGreetings(ctx context.Context) (int, error) {
	now := time.Now().UTC()
	items, err := s.recipients.DueForGreeting(ctx, now)
	if err != nil {
		return 0, err
	}
	sent := 0
	for i := range items {
		delivered, err := s.dispatchGreeting(ctx, &items[i], "", now)
		if err != nil {
			s.logger.Error("greeting dispatch failed",
				"recipient_id", items[i].ID, "error", err)
			continue
		}
		if delivered {
			sent++
		}
	}
	s.logger.Info("greeting scan finished", "candidates", len(items), "sent", sent)
	return sent, nil
}

// SendGreeting keeps the original single-recipient entry point compatible and
// routes it through the same idempotent period flow.
func (s *NotificationService) SendGreeting(ctx context.Context, recipient *model.CareRecipient, category string) error {
	_, err := s.dispatchGreeting(ctx, recipient, category, time.Now().UTC())
	return err
}

// dispatchGreeting returns delivered=true only when this caller actually sent
// the greeting. A duplicate (period already succeeded or a live lease) and an
// abandoned takeover both return delivered=false with no error.
func (s *NotificationService) dispatchGreeting(ctx context.Context, recipient *model.CareRecipient, category string, now time.Time) (bool, error) {
	periodKey := GreetingPeriodKey(recipient, now)
	claim, err := s.occupancy.Claim(ctx, constants.SMSKindGreeting, recipient.ID, 0, periodKey, "", occupancyLease, now)
	if err != nil {
		return false, err
	}
	if !claim.Acquired {
		s.logger.Info("greeting period already closed or in flight",
			"recipient_id", recipient.ID, "period_key", periodKey, "status", claim.Existing.Status)
		return false, nil
	}
	occ := claim.Occupancy

	// Post-claim recheck: the row could have been paused/stopped while waiting.
	fresh, err := s.recipients.Get(ctx, recipient.ID)
	if err != nil {
		_ = s.occupancy.MarkAbandoned(ctx, occ.ID, "recipient reload failed: "+err.Error(), time.Now().UTC())
		return false, err
	}
	if fresh.Status != constants.RecipientStatusActive {
		if err := s.occupancy.MarkAbandoned(ctx, occ.ID, "recipient no longer active", time.Now().UTC()); err != nil {
			return false, err
		}
		s.logger.Info("greeting abandoned, recipient not active", "recipient_id", recipient.ID)
		return false, nil
	}

	content, err := s.renderContent(ctx, fresh, category)
	if err != nil {
		_ = s.occupancy.MarkFailure(ctx, occ.ID, "render content: "+err.Error(), time.Now().UTC())
		return false, err
	}

	recipientID := fresh.ID
	occID := occ.ID
	logItem, sendErr := s.sms.Send(ctx, SMSMessage{
		CareRecipientID: &recipientID, RecipientPhone: fresh.Phone, Content: content,
		Kind: constants.SMSKindGreeting, PeriodKey: periodKey, OccupancyID: &occID,
	})
	finishAt := time.Now().UTC()
	if sendErr != nil {
		// Failure only records a failure; the period is left open for a later retry.
		if err := s.occupancy.MarkFailure(ctx, occ.ID, sendErr.Error(), finishAt); err != nil {
			return false, err
		}
		return false, sendErr
	}
	if err := s.occupancy.MarkSuccess(ctx, occ.ID, logItem.ID, finishAt); err != nil {
		return false, err
	}

	// LastGreetingAt stays a best-effort hint for the due scan; the occupancy
	// row remains the authoritative idempotency record.
	fresh.LastGreetingAt = &now
	if err := s.recipients.Update(ctx, fresh); err != nil {
		s.logger.Error("update last_greeting_at after successful greeting",
			"recipient_id", fresh.ID, "error", err)
	}
	s.logger.Info("greeting dispatched",
		"recipient_id", fresh.ID, "period_key", periodKey, "sms_log_id", logItem.ID)
	return true, nil
}

func (s *NotificationService) renderContent(ctx context.Context, recipient *model.CareRecipient, category string) (string, error) {
	template, err := s.templates.RandomActive(ctx, category)
	if err == repository.ErrNotFound {
		template = &model.SMSTemplate{Content: "{{name}}，您好，愿您今天平安顺心。如方便，请回复“已阅”报个平安。"}
	} else if err != nil {
		return "", err
	}
	return strings.ReplaceAll(template.Content, "{{name}}", recipient.Name), nil
}
