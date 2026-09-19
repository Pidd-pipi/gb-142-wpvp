package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
)

type SMSMessage struct {
	CareRecipientID      *uint
	FamilySubscriptionID *uint
	RecipientPhone       string
	Content              string
	Kind                 string
	// Idempotency coordinates of the owning greeting period / alert event.
	PeriodKey   string
	EventKey    string
	OccupancyID *uint
}

type SMSProvider interface {
	Send(context.Context, SMSMessage) error
}

// LogSMSSender is the default provider: it simulates delivery with a structured
// log line and always succeeds for a valid phone. Durable send records
// (including failures of other providers) are written by SMSService, keeping
// the provider free of persistence concerns.
type LogSMSSender struct {
	logger *slog.Logger
}

func NewLogSMSSender(logger *slog.Logger) *LogSMSSender {
	return &LogSMSSender{logger: logger}
}

func (s *LogSMSSender) Send(_ context.Context, message SMSMessage) error {
	if message.RecipientPhone == "" {
		return fmt.Errorf("send sms: recipient phone is empty")
	}
	s.logger.Info("sms sent by log provider",
		"kind", message.Kind, "recipient_phone", message.RecipientPhone,
		"period_key", message.PeriodKey, "event_key", message.EventKey)
	return nil
}

type SMSService struct {
	provider SMSProvider
	logs     *repository.SMSLogRepository
}

func NewSMSService(provider SMSProvider, logs *repository.SMSLogRepository) *SMSService {
	return &SMSService{provider: provider, logs: logs}
}

// Send dispatches the message and always writes a durable log row. On provider
// failure the row is marked failed with the reason and the error is returned,
// so callers can release (not close) their occupancy and retry later.
func (s *SMSService) Send(ctx context.Context, message SMSMessage) (*model.SMSLog, error) {
	now := time.Now().UTC()
	logItem := &model.SMSLog{
		CareRecipientID:      message.CareRecipientID,
		FamilySubscriptionID: message.FamilySubscriptionID,
		RecipientPhone:       message.RecipientPhone,
		MessageContent:       message.Content,
		Kind:                 message.Kind,
		PeriodKey:            message.PeriodKey,
		EventKey:             message.EventKey,
		OccupancyID:          message.OccupancyID,
		SentAt:               now,
	}
	sendErr := s.provider.Send(ctx, message)
	if sendErr != nil {
		logItem.Result = constants.SMSResultFailed
		logItem.FailureReason = sendErr.Error()
	} else {
		logItem.Result = constants.SMSResultSuccess
	}
	if err := s.logs.Create(ctx, logItem); err != nil {
		// Persisting the outcome is itself a failure surface; surface it so the
		// scan leaves the occupancy uncommitted and retries rather than
		// silently losing the record.
		return nil, fmt.Errorf("persist sms log: %w", err)
	}
	if sendErr != nil {
		return logItem, fmt.Errorf("deliver sms: %w", sendErr)
	}
	return logItem, nil
}
