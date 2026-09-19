package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"gorm.io/gorm"
)

type SendOccupancyRepository struct{ db *gorm.DB }

func NewSendOccupancyRepository(db *gorm.DB) *SendOccupancyRepository {
	return &SendOccupancyRepository{db: db}
}

// ClaimOutcome reports the result of an atomic Claim attempt.
type ClaimOutcome struct {
	// Acquired is true only when this caller now owns the lease and may send.
	Acquired bool
	// Existing is the current row when Acquired is false (nil when acquired).
	Existing *model.SendOccupancy
	// Reacquired marks a previously failed/abandoned/expired lease taken over for retry.
	Reacquired bool
	// Occupancy is the owned row (non-nil when Acquired is true).
	Occupancy *model.SendOccupancy
}

// Claim atomically reserves the dedup tuple (kind, recipient, subscription,
// period/event). Rules:
//   - no row yet                      -> insert a processing row and acquire it;
//   - success row                     -> refuse (the period/event is closed);
//   - processing row with live lease  -> refuse (a concurrent scan is sending);
//   - processing row with expired
//     lease, or failed/abandoned row  -> reacquire for (re)try.
//
// Abandoned rows are reacquirable: a confirmation that landed during the scan
// must not permanently close the subsequent overdue event.
func (r *SendOccupancyRepository) Claim(ctx context.Context, kind string, recipientID, subscriptionID uint, periodKey, eventKey string, lease time.Duration, now time.Time) (ClaimOutcome, error) {
	leaseUntil := now.Add(lease)
	row := &model.SendOccupancy{
		Kind: kind, CareRecipientID: recipientID, FamilySubscriptionID: subscriptionID,
		PeriodKey: periodKey, EventKey: eventKey,
		Status: constants.OccupancyProcessing, Attempts: 1, LastAttemptAt: &now, LeaseUntil: &leaseUntil,
	}
	err := r.db.WithContext(ctx).Create(row).Error
	if err == nil {
		return ClaimOutcome{Acquired: true, Occupancy: row}, nil
	}
	if !isDuplicateKeyErr(err) {
		return ClaimOutcome{}, fmt.Errorf("claim occupancy: %w", err)
	}

	existing, err := r.getByTuple(ctx, kind, recipientID, subscriptionID, periodKey, eventKey)
	if err != nil {
		return ClaimOutcome{}, err
	}

	if existing.Status == constants.OccupancySuccess || (existing.Status == constants.OccupancyProcessing && existing.LeaseUntil != nil && existing.LeaseUntil.After(now)) {
		return ClaimOutcome{Acquired: false, Existing: existing}, nil
	}

	// Stale/failed/abandoned: atomically take over the lease. Only one of the
	// competing retry callers can win this UPDATE.
	result := r.db.WithContext(ctx).Model(&model.SendOccupancy{}).
		Where("id = ? AND (status <> ? OR lease_until IS NULL OR lease_until <= ?)", existing.ID, constants.OccupancySuccess, now).
		Updates(map[string]any{
			"status":          constants.OccupancyProcessing,
			"lease_until":     leaseUntil,
			"last_attempt_at": now,
			"attempts":        gorm.Expr("attempts + 1"),
			"failure_reason":  "",
		})
	if result.Error != nil {
		return ClaimOutcome{}, fmt.Errorf("reacquire occupancy: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		fresh, err := r.getByTuple(ctx, kind, recipientID, subscriptionID, periodKey, eventKey)
		if err != nil {
			return ClaimOutcome{}, err
		}
		return ClaimOutcome{Acquired: false, Existing: fresh}, nil
	}
	existing.Status = constants.OccupancyProcessing
	existing.LeaseUntil = &leaseUntil
	existing.LastAttemptAt = &now
	existing.Attempts++
	existing.FailureReason = ""
	return ClaimOutcome{Acquired: true, Reacquired: true, Occupancy: existing}, nil
}

// MarkSuccess closes the period/event with its successful log. Any later Claim
// on the same tuple will be refused, so duplicates (concurrent scans, manual
// re-triggers, retries) never send again.
func (r *SendOccupancyRepository) MarkSuccess(ctx context.Context, id, smsLogID uint, now time.Time) error {
	result := r.db.WithContext(ctx).Model(&model.SendOccupancy{}).
		Where("id = ? AND status = ?", id, constants.OccupancyProcessing).
		Updates(map[string]any{
			"status":         constants.OccupancySuccess,
			"lease_until":    nil,
			"success_log_id": smsLogID,
			"failure_reason": "",
			"updated_at":     now,
		})
	if result.Error != nil {
		return fmt.Errorf("mark occupancy success: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkFailure only records the failure: it releases the lease without closing
// the period/event, so a later scan can reacquire and retry. No caller can see
// a "success" for this attempt.
func (r *SendOccupancyRepository) MarkFailure(ctx context.Context, id uint, reason string, now time.Time) error {
	result := r.db.WithContext(ctx).Model(&model.SendOccupancy{}).
		Where("id = ? AND status = ?", id, constants.OccupancyProcessing).
		Updates(map[string]any{
			"status":         constants.OccupancyFailed,
			"lease_until":    nil,
			"failure_reason": reason,
			"updated_at":     now,
		})
	if result.Error != nil {
		return fmt.Errorf("mark occupancy failure: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkAbandoned gives up an acquired lease without sending and without closing
// the period/event. Used when a confirmation arrives during the scan: this
// alert is dropped and the event stays free for a future overdue period.
func (r *SendOccupancyRepository) MarkAbandoned(ctx context.Context, id uint, reason string, now time.Time) error {
	result := r.db.WithContext(ctx).Model(&model.SendOccupancy{}).
		Where("id = ? AND status = ?", id, constants.OccupancyProcessing).
		Updates(map[string]any{
			"status":         constants.OccupancyAbandoned,
			"lease_until":    nil,
			"failure_reason": reason,
			"updated_at":     now,
		})
	if result.Error != nil {
		return fmt.Errorf("mark occupancy abandoned: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *SendOccupancyRepository) getByTuple(ctx context.Context, kind string, recipientID, subscriptionID uint, periodKey, eventKey string) (*model.SendOccupancy, error) {
	var item model.SendOccupancy
	err := r.db.WithContext(ctx).
		Where("kind = ? AND care_recipient_id = ? AND family_subscription_id = ? AND period_key = ? AND event_key = ?",
			kind, recipientID, subscriptionID, periodKey, eventKey).
		First(&item).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get occupancy: %w", err)
	}
	return &item, nil
}

// List supports read-back of period/event ownership, result and SMS linkage.
func (r *SendOccupancyRepository) List(ctx context.Context, page, pageSize int, recipientID *uint, kind string) ([]model.SendOccupancy, int64, error) {
	var items []model.SendOccupancy
	var total int64
	q := r.db.WithContext(ctx).Model(&model.SendOccupancy{})
	if recipientID != nil {
		q = q.Where("care_recipient_id = ?", *recipientID)
	}
	if kind != "" {
		q = q.Where("kind = ?", kind)
	}
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count send occupancies: %w", err)
	}
	if err := q.Order("id desc").Offset((page - 1) * pageSize).Limit(pageSize).Find(&items).Error; err != nil {
		return nil, 0, fmt.Errorf("list send occupancies: %w", err)
	}
	return items, total, nil
}

// isDuplicateKeyErr recognizes unique-constraint violations from MySQL and SQLite.
func isDuplicateKeyErr(err error) bool {
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate entry") || strings.Contains(msg, "unique constraint")
}
