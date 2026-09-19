package model

import "time"

type SMSLog struct {
	ID                   uint   `gorm:"primaryKey" json:"id"`
	CareRecipientID      *uint  `gorm:"index" json:"care_recipient_id"`
	FamilySubscriptionID *uint  `gorm:"index" json:"family_subscription_id"`
	RecipientPhone       string `gorm:"size:32;not null;index" json:"recipient_phone"`
	MessageContent       string `gorm:"type:text;not null" json:"message_content"`
	Kind                 string `gorm:"size:16;not null;index" json:"kind"`
	Result               string `gorm:"size:16;not null;index" json:"result"`
	FailureReason        string `gorm:"type:text" json:"failure_reason,omitempty"`
	// Dedup coordinates: the greeting period or alert event this log belongs to.
	PeriodKey string `gorm:"size:32;not null;default:'';index" json:"period_key"`
	EventKey  string `gorm:"size:64;not null;default:'';index" json:"event_key"`
	// OccupancyID links the log back to its period/event ownership row.
	OccupancyID *uint     `gorm:"index" json:"occupancy_id,omitempty"`
	SentAt      time.Time `gorm:"not null;index" json:"sent_at"`
}
