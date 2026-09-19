package model

import "time"

type SMSLog struct {
	ID                   uint           `gorm:"primaryKey" json:"id"`
	CareRecipientID      *uint          `gorm:"index" json:"care_recipient_id"`
	FamilySubscriptionID *uint          `gorm:"index" json:"family_subscription_id"`
	RecipientPhone       string         `gorm:"size:32;not null;index" json:"recipient_phone"`
	MessageContent       string         `gorm:"type:text;not null" json:"message_content"`
	Kind                 string         `gorm:"size:16;not null;index" json:"kind"`
	PeriodKey            string         `gorm:"size:64;index" json:"period_key"`
	ClaimID              *uint          `gorm:"index" json:"claim_id"`
	Claim                *DispatchClaim `gorm:"foreignKey:ClaimID" json:"claim,omitempty"`
	Result               string         `gorm:"size:16;not null;index" json:"result"`
	FailureReason        string         `gorm:"type:text" json:"failure_reason,omitempty"`
	SentAt               time.Time      `gorm:"not null;index" json:"sent_at"`
}
