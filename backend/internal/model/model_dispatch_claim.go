package model

import "time"

// DispatchClaim 是一次问候/告警发送的占位记录。
// 唯一索引 (care_recipient_id, family_subscription_id, kind, period_key)
// 保证同一关怀对象在同一周期（问候）或同一事件（告警）中只存在一条占位，
// 并发扫描、重复触发时只有一个执行流能占位成功。
// FamilySubscriptionID 为 0 表示问候（不针对订阅），避免唯一索引中的 NULL 不参与去重。
type DispatchClaim struct {
	ID                   uint      `gorm:"primaryKey" json:"id"`
	CareRecipientID      uint      `gorm:"not null;uniqueIndex:uniq_dispatch_claim,priority:1;index" json:"care_recipient_id"`
	FamilySubscriptionID uint      `gorm:"not null;default:0;uniqueIndex:uniq_dispatch_claim,priority:2;index" json:"family_subscription_id"`
	Kind                 string    `gorm:"size:16;not null;uniqueIndex:uniq_dispatch_claim,priority:3;index" json:"kind"`
	PeriodKey            string    `gorm:"size:64;not null;uniqueIndex:uniq_dispatch_claim,priority:4;index" json:"period_key"`
	State                string    `gorm:"size:16;not null;index" json:"state"`
	SMSLogID             *uint     `gorm:"index" json:"sms_log_id,omitempty"`
	FailureReason        string    `gorm:"type:text" json:"failure_reason,omitempty"`
	Version              int       `gorm:"not null;default:1" json:"version"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}
