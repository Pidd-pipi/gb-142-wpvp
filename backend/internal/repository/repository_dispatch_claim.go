package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	mysqldriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

// claimedStaleTimeout 是占位（claimed）状态的最大存活时间。
// 进程在发送途中崩溃会留下 claimed 占位，超过该时长后允许其它扫描回收重试，
// 避免一次崩溃导致该周期/事件永久无法发送。
const claimedStaleTimeout = 10 * time.Minute

type DispatchClaimRepository struct{ db *gorm.DB }

func NewDispatchClaimRepository(db *gorm.DB) *DispatchClaimRepository {
	return &DispatchClaimRepository{db: db}
}

// TryAcquire 尝试为 (对象, 订阅, 类型, 周期/事件) 占位。
// 返回 acquired=true 表示调用方获得发送权；acquired=false 表示该周期/事件
// 已有成功记录或正被其它执行流占用，本次必须放弃发送。
// failed/released 状态的旧占位可被重新认领（失败重试、放弃不占事件）；
// 超时未完结的 claimed 占位视为崩溃残留，同样允许回收。
func (r *DispatchClaimRepository) TryAcquire(ctx context.Context, recipientID, subscriptionID uint, kind, periodKey string, now time.Time) (*model.DispatchClaim, bool, error) {
	claim := &model.DispatchClaim{
		CareRecipientID:      recipientID,
		FamilySubscriptionID: subscriptionID,
		Kind:                 kind,
		PeriodKey:            periodKey,
		State:                constants.ClaimStateClaimed,
		Version:              1,
	}
	if err := r.db.WithContext(ctx).Create(claim).Error; err != nil {
		if !isDuplicateKey(err) {
			return nil, false, fmt.Errorf("insert dispatch claim: %w", err)
		}
	} else {
		return claim, true, nil
	}
	// 已存在占位：仅允许认领可重试状态（failed/released）或崩溃残留的 claimed。
	staleBefore := now.Add(-claimedStaleTimeout)
	result := r.db.WithContext(ctx).Model(&model.DispatchClaim{}).
		Where("care_recipient_id = ? AND family_subscription_id = ? AND kind = ? AND period_key = ?", recipientID, subscriptionID, kind, periodKey).
		Where("state IN ? OR (state = ? AND updated_at < ?)",
			[]string{constants.ClaimStateFailed, constants.ClaimStateReleased}, constants.ClaimStateClaimed, staleBefore).
		Updates(map[string]any{
			"state":          constants.ClaimStateClaimed,
			"sms_log_id":     nil,
			"failure_reason": "",
			"version":        gorm.Expr("version + 1"),
			"updated_at":     now,
		})
	if result.Error != nil {
		return nil, false, fmt.Errorf("reclaim dispatch claim: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return nil, false, nil
	}
	var fresh model.DispatchClaim
	err := r.db.WithContext(ctx).
		Where("care_recipient_id = ? AND family_subscription_id = ? AND kind = ? AND period_key = ?", recipientID, subscriptionID, kind, periodKey).
		First(&fresh).Error
	if err != nil {
		return nil, false, fmt.Errorf("load reclaimed dispatch claim: %w", err)
	}
	return &fresh, true, nil
}

// MarkSent 将占位置为成功终态并关联发送日志；仅 claimed 状态可流转，防止并发下互相覆盖。
func (r *DispatchClaimRepository) MarkSent(ctx context.Context, claimID uint, smsLogID uint) error {
	return r.transition(ctx, claimID, map[string]any{
		"state":          constants.ClaimStateSent,
		"sms_log_id":     smsLogID,
		"failure_reason": "",
	})
}

// MarkFailed 记录失败原因并释放占位，后续扫描可重新认领重试。
func (r *DispatchClaimRepository) MarkFailed(ctx context.Context, claimID uint, reason string) error {
	return r.transition(ctx, claimID, map[string]any{
		"state":          constants.ClaimStateFailed,
		"failure_reason": reason,
	})
}

// MarkReleased 放弃本次发送并释放占位（如扫描期间完成确认），不占用事件额度。
func (r *DispatchClaimRepository) MarkReleased(ctx context.Context, claimID uint, reason string) error {
	return r.transition(ctx, claimID, map[string]any{
		"state":          constants.ClaimStateReleased,
		"failure_reason": reason,
	})
}

func (r *DispatchClaimRepository) transition(ctx context.Context, claimID uint, fields map[string]any) error {
	result := r.db.WithContext(ctx).Model(&model.DispatchClaim{}).
		Where("id = ? AND state = ?", claimID, constants.ClaimStateClaimed).
		Updates(fields)
	if result.Error != nil {
		return fmt.Errorf("update dispatch claim %d: %w", claimID, result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("update dispatch claim %d: %w", claimID, ErrClaimStateConflict)
	}
	return nil
}

// ListByPeriodKeys 批量回读一组对象在指定周期/事件下的占位，供状态查询展示占用关系。
func (r *DispatchClaimRepository) ListByPeriodKeys(ctx context.Context, recipientIDs []uint, periodKeys []string) ([]model.DispatchClaim, error) {
	if len(recipientIDs) == 0 || len(periodKeys) == 0 {
		return nil, nil
	}
	var items []model.DispatchClaim
	err := r.db.WithContext(ctx).
		Where("care_recipient_id IN ? AND period_key IN ?", recipientIDs, periodKeys).
		Order("id desc").
		Find(&items).Error
	if err != nil {
		return nil, fmt.Errorf("list dispatch claims: %w", err)
	}
	return items, nil
}

func isDuplicateKey(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == 1062
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") || strings.Contains(msg, "Duplicate entry")
}
