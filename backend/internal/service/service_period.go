package service

import (
	"fmt"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
)

// frequencyInterval mirrors the cadence used to decide when a greeting is due.
func frequencyInterval(frequency string) time.Duration {
	switch frequency {
	case constants.FrequencyWeekly:
		return 7 * 24 * time.Hour
	case constants.FrequencyMonthly:
		return 30 * 24 * time.Hour
	default:
		return 24 * time.Hour
	}
}

// GreetingPeriodKey returns the stable identifier of the unique care period a
// greeting belongs to. Buckets are anchored at care_start_at and advance by the
// configured frequency, so all scans inside the same period produce the same
// key and therefore at most one successful greeting.
func GreetingPeriodKey(recipient *model.CareRecipient, now time.Time) string {
	interval := frequencyInterval(recipient.CareFrequency)
	start := recipient.CareStartAt.UTC()
	elapsed := now.Sub(start)
	if elapsed < 0 {
		elapsed = 0
	}
	bucket := elapsed / interval
	return fmt.Sprintf("%s-%d-%d", recipient.CareFrequency, start.Unix(), int64(bucket))
}

// AlertEventKey derives the unique overdue event from the recipient's latest
// confirmation. Each new confirmation opens a new event key, so one alert event
// can ever produce one successful record. A recipient who has never confirmed
// shares one stable "never" event.
func AlertEventKey(lastConfirmedAt *time.Time) string {
	if lastConfirmedAt == nil {
		return "never-confirmed"
	}
	return fmt.Sprintf("confirm-%d", lastConfirmedAt.UTC().Unix())
}

// isOverdueAt reports whether the recipient is past the confirmation timeout at
// instant now. It mirrors the repository Overdue predicate and is used for the
// post-claim recheck: a confirmation landing during the scan turns this false.
func isOverdueAt(recipient *model.CareRecipient, cutoff time.Time) bool {
	if recipient.Status != constants.RecipientStatusActive || recipient.CareStartAt.After(cutoff) {
		return false
	}
	return recipient.LastConfirmedAt == nil || recipient.LastConfirmedAt.Before(cutoff)
}
