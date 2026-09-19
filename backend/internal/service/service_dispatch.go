package service

import (
	"fmt"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
)

// DispatchSummary 汇总一轮扫描的占位结果，供调度器、手工触发接口和日志回读。
type DispatchSummary struct {
	Sent     int `json:"sent"`
	Skipped  int `json:"skipped"`
	Failed   int `json:"failed"`
	Released int `json:"released"`
}

// ClaimView 是占位记录在状态/日志回读中的视图，体现周期、事件、结果与占用关系。
type ClaimView struct {
	ID                   uint      `json:"id"`
	Kind                 string    `json:"kind"`
	PeriodKey            string    `json:"period_key"`
	State                string    `json:"state"`
	FamilySubscriptionID uint      `json:"family_subscription_id"`
	SMSLogID             *uint     `json:"sms_log_id,omitempty"`
	FailureReason        string    `json:"failure_reason,omitempty"`
	UpdatedAt            time.Time `json:"updated_at"`
}

func NewClaimView(claim *model.DispatchClaim) *ClaimView {
	if claim == nil {
		return nil
	}
	return &ClaimView{
		ID:                   claim.ID,
		Kind:                 claim.Kind,
		PeriodKey:            claim.PeriodKey,
		State:                claim.State,
		FamilySubscriptionID: claim.FamilySubscriptionID,
		SMSLogID:             claim.SMSLogID,
		FailureReason:        claim.FailureReason,
		UpdatedAt:            claim.UpdatedAt,
	}
}

// GreetingPeriodKey 按关怀频率把问候归入唯一自然周期：
// daily 为自然日、weekly 为 ISO 周、monthly 为自然月。
func GreetingPeriodKey(frequency string, now time.Time) string {
	now = now.UTC()
	switch frequency {
	case constants.FrequencyWeekly:
		year, week := now.ISOWeek()
		return fmt.Sprintf("%04d-W%02d", year, week)
	case constants.FrequencyMonthly:
		return now.Format("2006-01")
	default:
		return now.Format("2006-01-02")
	}
}

// AlertEventKey 以最近确认时间（无确认时取关怀开始时间）形成唯一告警事件；
// 每次新的确认都会开启新事件，同一事件内不会重复告警。
func AlertEventKey(anchor time.Time) string {
	return fmt.Sprintf("confirm:%d", anchor.UTC().Unix())
}

// AlertAnchor 返回告警事件锚点：最近确认时间，未确认过则为关怀开始时间。
func AlertAnchor(recipient *model.CareRecipient) time.Time {
	if recipient.LastConfirmedAt != nil {
		return recipient.LastConfirmedAt.UTC()
	}
	return recipient.CareStartAt.UTC()
}
