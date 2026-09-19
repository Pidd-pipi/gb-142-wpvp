package model

import "time"

// SendOccupancy records the idempotency ownership of one greeting period or one
// overdue alert event. A unique index on the dedup tuple guarantees at most one
// row per recipient (and family subscription) per period/event, even when
// multiple scans run concurrently.
type SendOccupancy struct {
	ID                   uint   `gorm:"primaryKey" json:"id"`
	Kind                 string `gorm:"size:16;not null;uniqueIndex:idx_send_occupancy_dedup,priority:1" json:"kind"`
	CareRecipientID      uint   `gorm:"not null;uniqueIndex:idx_send_occupancy_dedup,priority:2" json:"care_recipient_id"`
	FamilySubscriptionID uint   `gorm:"not null;default:0;uniqueIndex:idx_send_occupancy_dedup,priority:3" json:"family_subscription_id"`
	// PeriodKey identifies the unique care period for a greeting (empty for alerts).
	PeriodKey string `gorm:"size:32;not null;default:'';uniqueIndex:idx_send_occupancy_dedup,priority:4" json:"period_key"`
	// EventKey identifies the unique overdue event derived from the latest confirmation (empty for greetings).
	EventKey string `gorm:"size:64;not null;default:'';uniqueIndex:idx_send_occupancy_dedup,priority:5" json:"event_key"`
	// Status is one of: processing / success / failed / abandoned.
	Status        string     `gorm:"size:16;not null;index" json:"status"`
	Attempts      int        `gorm:"not null;default:0" json:"attempts"`
	LastAttemptAt *time.Time `json:"last_attempt_at"`
	LeaseUntil    *time.Time `gorm:"index" json:"lease_until,omitempty"`
	SuccessLogID  *uint      `json:"success_log_id"`
	FailureReason string     `gorm:"type:text" json:"failure_reason,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}
