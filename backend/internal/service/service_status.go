package service

import (
	"context"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
)

type RecipientStatus struct {
	Recipient model.CareRecipient `json:"recipient"`
	State     string              `json:"state"`
	Reason    string              `json:"reason"`
	// GreetingPeriod 是当前问候周期标识，GreetingClaim 是该周期的占用与结果（可能为空）。
	GreetingPeriod string     `json:"greeting_period"`
	GreetingClaim  *ClaimView `json:"greeting_claim,omitempty"`
	// AlertEvent 是当前告警事件标识（由最近确认形成），AlertClaims 是该事件下各订阅的占用与结果。
	AlertEvent  string      `json:"alert_event"`
	AlertClaims []ClaimView `json:"alert_claims,omitempty"`
}

type StatusService struct {
	recipients *repository.RecipientRepository
	claims     *repository.DispatchClaimRepository
	timeout    time.Duration
}

func NewStatusService(recipients *repository.RecipientRepository, claims *repository.DispatchClaimRepository, timeout time.Duration) *StatusService {
	return &StatusService{recipients: recipients, claims: claims, timeout: timeout}
}

func (s *StatusService) List(ctx context.Context, page, pageSize int) ([]RecipientStatus, int64, error) {
	items, total, err := s.recipients.List(ctx, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	now := time.Now().UTC()
	cutoff := now.Add(-s.timeout)
	result := make([]RecipientStatus, 0, len(items))
	recipientIDs := make([]uint, 0, len(items))
	periodKeys := make([]string, 0, len(items)*2)
	seen := map[string]struct{}{}
	for i := range items {
		item := &items[i]
		state, reason := "失联", "超过确认时限"
		switch {
		case item.Status == constants.RecipientStatusPaused:
			state, reason = "已暂停", "关怀已暂停"
		case item.LastConfirmedAt != nil && !item.LastConfirmedAt.Before(cutoff):
			state, reason = "平安", "近期已确认"
		case item.LastConfirmedAt == nil && item.CareStartAt.After(cutoff):
			state, reason = "待确认", "尚未确认"
		}
		greetingPeriod := GreetingPeriodKey(item.CareFrequency, now)
		alertEvent := AlertEventKey(AlertAnchor(item))
		result = append(result, RecipientStatus{Recipient: *item, State: state, Reason: reason, GreetingPeriod: greetingPeriod, AlertEvent: alertEvent})
		recipientIDs = append(recipientIDs, item.ID)
		for _, key := range []string{greetingPeriod, alertEvent} {
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				periodKeys = append(periodKeys, key)
			}
		}
	}
	claims, err := s.claims.ListByPeriodKeys(ctx, recipientIDs, periodKeys)
	if err != nil {
		return nil, 0, err
	}
	// 按 (对象, 类型, 周期/事件) 归集占位；告警事件下每个订阅各一条。
	type claimKey struct {
		recipientID uint
		kind        string
		periodKey   string
	}
	grouped := map[claimKey][]model.DispatchClaim{}
	for _, claim := range claims {
		key := claimKey{claim.CareRecipientID, claim.Kind, claim.PeriodKey}
		grouped[key] = append(grouped[key], claim)
	}
	for i := range result {
		entry := &result[i]
		if list := grouped[claimKey{entry.Recipient.ID, constants.SMSKindGreeting, entry.GreetingPeriod}]; len(list) > 0 {
			entry.GreetingClaim = NewClaimView(&list[0])
		}
		for j := range grouped[claimKey{entry.Recipient.ID, constants.SMSKindAlert, entry.AlertEvent}] {
			claim := grouped[claimKey{entry.Recipient.ID, constants.SMSKindAlert, entry.AlertEvent}][j]
			entry.AlertClaims = append(entry.AlertClaims, *NewClaimView(&claim))
		}
	}
	return result, total, nil
}
