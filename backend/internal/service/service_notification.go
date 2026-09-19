package service

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
)

type NotificationService struct {
	recipients *repository.RecipientRepository
	templates  *repository.TemplateRepository
	claims     *repository.DispatchClaimRepository
	sms        *SMSService
	logger     *slog.Logger
}

func NewNotificationService(recipients *repository.RecipientRepository, templates *repository.TemplateRepository, claims *repository.DispatchClaimRepository, sms *SMSService, logger *slog.Logger) *NotificationService {
	return &NotificationService{recipients: recipients, templates: templates, claims: claims, sms: sms, logger: logger}
}

// SendDueGreetings 扫描到期对象并发送问候。每个对象按关怀频率形成唯一周期，
// 同一周期内只占位一次：并发扫描与重复触发只有一个执行流能发送，
// 发送失败只记失败并释放占位，后续扫描可重试。
func (s *NotificationService) SendDueGreetings(ctx context.Context) (DispatchSummary, error) {
	var summary DispatchSummary
	now := time.Now().UTC()
	items, err := s.recipients.DueForGreeting(ctx, now)
	if err != nil {
		return summary, err
	}
	for i := range items {
		periodKey := GreetingPeriodKey(items[i].CareFrequency, now)
		claim, acquired, err := s.claims.TryAcquire(ctx, items[i].ID, 0, constants.SMSKindGreeting, periodKey, now)
		if err != nil {
			summary.Failed++
			s.logger.Error("acquire greeting claim", "recipient_id", items[i].ID, "period_key", periodKey, "error", err)
			continue
		}
		if !acquired {
			summary.Skipped++
			continue
		}
		if err := s.sendGreeting(ctx, &items[i], claim, periodKey, now); err != nil {
			summary.Failed++
			s.logger.Error("send greeting", "recipient_id", items[i].ID, "period_key", periodKey, "error", err)
			continue
		}
		summary.Sent++
	}
	s.logger.Info("due greetings dispatched", "sent", summary.Sent, "skipped", summary.Skipped, "failed", summary.Failed)
	return summary, nil
}

func (s *NotificationService) sendGreeting(ctx context.Context, recipient *model.CareRecipient, claim *model.DispatchClaim, periodKey string, now time.Time) error {
	template, err := s.templates.RandomActive(ctx, "")
	if err == repository.ErrNotFound {
		template = &model.SMSTemplate{Content: "{{name}}，您好，愿您今天平安顺心。如方便，请回复“已阅”报个平安。"}
	} else if err != nil {
		if markErr := s.claims.MarkFailed(ctx, claim.ID, err.Error()); markErr != nil {
			s.logger.Warn("release greeting claim", "claim_id", claim.ID, "error", markErr)
		}
		return err
	}
	content := strings.ReplaceAll(template.Content, "{{name}}", recipient.Name)
	recipientID := recipient.ID
	logItem, err := s.sms.Send(ctx, SMSMessage{
		CareRecipientID: &recipientID,
		RecipientPhone:  recipient.Phone,
		Content:         content,
		Kind:            constants.SMSKindGreeting,
		PeriodKey:       periodKey,
		ClaimID:         &claim.ID,
	})
	if err != nil {
		if markErr := s.claims.MarkFailed(ctx, claim.ID, err.Error()); markErr != nil {
			s.logger.Warn("mark greeting claim failed", "claim_id", claim.ID, "error", markErr)
		}
		return err
	}
	if err := s.claims.MarkSent(ctx, claim.ID, logItem.ID); err != nil {
		return err
	}
	// LastGreetingAt 仅用于到期预筛，幂等由占位保证；更新失败不影响本次闭环。
	recipient.LastGreetingAt = &now
	if err := s.recipients.Update(ctx, recipient); err != nil {
		s.logger.Warn("update last greeting time", "recipient_id", recipient.ID, "error", err)
	}
	s.logger.Info("greeting dispatched", "recipient_id", recipient.ID, "period_key", periodKey, "claim_id", claim.ID)
	return nil
}
