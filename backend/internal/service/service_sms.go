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
	PeriodKey            string
	ClaimID              *uint
}

// SMSProvider 只负责把短信发出去；发送日志由 SMSService 统一落库，
// 保证成功/失败结果与失败原因在任何 provider 实现下都有一致的记录。
type SMSProvider interface {
	Send(context.Context, SMSMessage) error
}

// LogSMSSender 是默认的模拟 provider：只输出结构化日志并视为发送成功，
// 生产环境可替换为真实短信服务商实现。
type LogSMSSender struct{ logger *slog.Logger }

func NewLogSMSSender(logger *slog.Logger) *LogSMSSender { return &LogSMSSender{logger: logger} }

func (s *LogSMSSender) Send(_ context.Context, message SMSMessage) error {
	if message.RecipientPhone == "" {
		return fmt.Errorf("send sms: recipient phone is empty")
	}
	s.logger.Info("sms sent by log provider", "kind", message.Kind, "period_key", message.PeriodKey, "recipient_phone", message.RecipientPhone)
	return nil
}

type SMSService struct {
	provider SMSProvider
	logs     *repository.SMSLogRepository
	logger   *slog.Logger
}

func NewSMSService(provider SMSProvider, logs *repository.SMSLogRepository, logger *slog.Logger) *SMSService {
	return &SMSService{provider: provider, logs: logs, logger: logger}
}

// Send 调用 provider 发送短信并记录发送日志；返回的日志条目无论成败都会落库，
// 失败时同时返回错误，调用方据此释放占位以便后续扫描重试。
func (s *SMSService) Send(ctx context.Context, message SMSMessage) (*model.SMSLog, error) {
	logItem := &model.SMSLog{
		CareRecipientID:      message.CareRecipientID,
		FamilySubscriptionID: message.FamilySubscriptionID,
		RecipientPhone:       message.RecipientPhone,
		MessageContent:       message.Content,
		Kind:                 message.Kind,
		PeriodKey:            message.PeriodKey,
		ClaimID:              message.ClaimID,
		SentAt:               time.Now().UTC(),
	}
	sendErr := s.provider.Send(ctx, message)
	if sendErr != nil {
		logItem.Result = constants.SMSResultFailed
		logItem.FailureReason = sendErr.Error()
	} else {
		logItem.Result = constants.SMSResultSuccess
	}
	if err := s.logs.Create(ctx, logItem); err != nil {
		return nil, fmt.Errorf("record sms log: %w", err)
	}
	if sendErr != nil {
		s.logger.Warn("sms send failed", "sms_log_id", logItem.ID, "kind", message.Kind, "period_key", message.PeriodKey, "error", sendErr)
		return logItem, sendErr
	}
	return logItem, nil
}
