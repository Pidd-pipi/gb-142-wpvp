package service

import (
	"context"

	"github.com/blueship581/gbcarenotify/internal/model"
	"github.com/blueship581/gbcarenotify/internal/repository"
)

// OccupancyService provides read-back of greeting periods and alert events:
// their dedup coordinates, lifecycle result and the successful log they own.
type OccupancyService struct {
	repo *repository.SendOccupancyRepository
}

func NewOccupancyService(repo *repository.SendOccupancyRepository) *OccupancyService {
	return &OccupancyService{repo: repo}
}

func (s *OccupancyService) List(ctx context.Context, page, pageSize int, recipientID *uint, kind string) ([]model.SendOccupancy, int64, error) {
	return s.repo.List(ctx, page, pageSize, recipientID, kind)
}
