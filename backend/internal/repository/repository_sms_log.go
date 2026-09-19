package repository

import (
	"context"
	"errors"
	"fmt"
	"github.com/blueship581/gbcarenotify/internal/model"
	"gorm.io/gorm"
)

// SMSLogFilter narrows an SMS log query. Zero values mean "no filter",
// preserving the original recipient-only filtering behavior.
type SMSLogFilter struct {
	CareRecipientID *uint
	Kind            string
	Result          string
}

type SMSLogRepository struct{ db *gorm.DB }

func NewSMSLogRepository(db *gorm.DB) *SMSLogRepository { return &SMSLogRepository{db: db} }
func (r *SMSLogRepository) Create(ctx context.Context, item *model.SMSLog) error {
	if err := r.db.WithContext(ctx).Create(item).Error; err != nil {
		return fmt.Errorf("create sms log: %w", err)
	}
	return nil
}
func (r *SMSLogRepository) Get(ctx context.Context, id uint) (*model.SMSLog, error) {
	var item model.SMSLog
	err := r.db.WithContext(ctx).First(&item, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get sms log: %w", err)
	}
	return &item, nil
}
func (r *SMSLogRepository) List(ctx context.Context, page, pageSize int, filter SMSLogFilter) ([]model.SMSLog, int64, error) {
	var items []model.SMSLog
	var total int64
	q := r.db.WithContext(ctx).Model(&model.SMSLog{})
	if filter.CareRecipientID != nil {
		q = q.Where("care_recipient_id = ?", *filter.CareRecipientID)
	}
	if filter.Kind != "" {
		q = q.Where("kind = ?", filter.Kind)
	}
	if filter.Result != "" {
		q = q.Where("result = ?", filter.Result)
	}
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count sms logs: %w", err)
	}
	if err := q.Order("sent_at desc").Offset((page - 1) * pageSize).Limit(pageSize).Find(&items).Error; err != nil {
		return nil, 0, fmt.Errorf("list sms logs: %w", err)
	}
	return items, total, nil
}
