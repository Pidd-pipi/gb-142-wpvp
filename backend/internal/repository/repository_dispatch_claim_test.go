package repository

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openClaimTestDB(t *testing.T, dsn string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := model.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestDispatchClaimLifecycle(t *testing.T) {
	db := openClaimTestDB(t, ":memory:")
	repo := NewDispatchClaimRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()

	claim, acquired, err := repo.TryAcquire(ctx, 7, 0, constants.SMSKindGreeting, "2026-09-19", now)
	if err != nil || !acquired {
		t.Fatalf("first acquire: acquired=%v err=%v", acquired, err)
	}
	// 占位在途（claimed）时，重复触发不得再次获得。
	if _, acquired, err = repo.TryAcquire(ctx, 7, 0, constants.SMSKindGreeting, "2026-09-19", now); err != nil || acquired {
		t.Fatalf("second acquire while claimed: acquired=%v err=%v", acquired, err)
	}
	// 失败释放占位后，后续扫描可重新认领重试。
	if err = repo.MarkFailed(ctx, claim.ID, "provider outage"); err != nil {
		t.Fatal(err)
	}
	claim, acquired, err = repo.TryAcquire(ctx, 7, 0, constants.SMSKindGreeting, "2026-09-19", now)
	if err != nil || !acquired {
		t.Fatalf("reacquire after failure: acquired=%v err=%v", acquired, err)
	}
	// 成功为终态：同周期不再允许任何认领。
	if err = repo.MarkSent(ctx, claim.ID, 42); err != nil {
		t.Fatal(err)
	}
	if _, acquired, err = repo.TryAcquire(ctx, 7, 0, constants.SMSKindGreeting, "2026-09-19", now); err != nil || acquired {
		t.Fatalf("acquire after sent: acquired=%v err=%v", acquired, err)
	}
	var stored model.DispatchClaim
	if err = db.First(&stored, claim.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.State != constants.ClaimStateSent || stored.SMSLogID == nil || *stored.SMSLogID != 42 {
		t.Fatalf("sent claim=%+v", stored)
	}
}

func TestDispatchClaimReleaseDoesNotOccupyEvent(t *testing.T) {
	db := openClaimTestDB(t, ":memory:")
	repo := NewDispatchClaimRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()

	claim, acquired, err := repo.TryAcquire(ctx, 9, 3, constants.SMSKindAlert, "confirm:1726", now)
	if err != nil || !acquired {
		t.Fatalf("acquire: acquired=%v err=%v", acquired, err)
	}
	// 扫描期间完成确认 → 放弃告警并释放占位，事件不被占用。
	if err = repo.MarkReleased(ctx, claim.ID, "confirmed during scan"); err != nil {
		t.Fatal(err)
	}
	if _, acquired, err = repo.TryAcquire(ctx, 9, 3, constants.SMSKindAlert, "confirm:1726", now); err != nil || !acquired {
		t.Fatalf("reacquire after release: acquired=%v err=%v", acquired, err)
	}
}

func TestDispatchClaimReclaimStaleClaimed(t *testing.T) {
	db := openClaimTestDB(t, ":memory:")
	repo := NewDispatchClaimRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()

	if _, acquired, err := repo.TryAcquire(ctx, 5, 0, constants.SMSKindGreeting, "2026-09-19", now); err != nil || !acquired {
		t.Fatalf("acquire: acquired=%v err=%v", acquired, err)
	}
	// 模拟发送途中崩溃：占位停留在 claimed 且长时间未更新，允许回收重试。
	stale := now.Add(-30 * time.Minute)
	if err := db.Model(&model.DispatchClaim{}).Where("kind = ? AND period_key = ?", constants.SMSKindGreeting, "2026-09-19").Update("updated_at", stale).Error; err != nil {
		t.Fatal(err)
	}
	if _, acquired, err := repo.TryAcquire(ctx, 5, 0, constants.SMSKindGreeting, "2026-09-19", now); err != nil || !acquired {
		t.Fatalf("reclaim stale claimed: acquired=%v err=%v", acquired, err)
	}
}

func TestDispatchClaimConcurrentAcquire(t *testing.T) {
	// 共享缓存的内存库表锁不走 busy 重试，用临时文件库模拟多连接并发。
	db := openClaimTestDB(t, "file:"+t.TempDir()+"/claim.db?_busy_timeout=5000&_journal_mode=WAL")
	repo := NewDispatchClaimRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()

	const workers = 16
	var wg sync.WaitGroup
	wins := make(chan bool, workers)
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, acquired, err := repo.TryAcquire(ctx, 11, 0, constants.SMSKindGreeting, "2026-09-19", now)
			if err != nil {
				errs <- err
				return
			}
			wins <- acquired
		}()
	}
	wg.Wait()
	close(wins)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent acquire error: %v", err)
	}
	acquiredCount := 0
	for acquired := range wins {
		if acquired {
			acquiredCount++
		}
	}
	if acquiredCount != 1 {
		t.Fatalf("concurrent acquire winners=%d, want exactly 1", acquiredCount)
	}
}
