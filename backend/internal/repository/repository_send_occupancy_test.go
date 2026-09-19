package repository

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blueship581/gbcarenotify/internal/constants"
	"github.com/blueship581/gbcarenotify/internal/model"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// newSerialDB returns an in-memory SQLite DB shared across connections and
// pinned to a single connection so concurrent goroutines exercise the unique
// index and atomic claim transitions without "database is locked" errors.
func newSerialDB(t *testing.T) *gorm.DB {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", name)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := model.AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestSendOccupancyClaimIsExclusive(t *testing.T) {
	db := newSerialDB(t)
	repo := NewSendOccupancyRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()

	const runners = 16
	var wg sync.WaitGroup
	acquired := make(chan ClaimOutcome, runners)
	for i := 0; i < runners; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			outcome, err := repo.Claim(ctx, constants.SMSKindGreeting, 7, 0, "daily-1-0", "", time.Minute, now)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			if outcome.Acquired {
				acquired <- outcome
			}
		}()
	}
	wg.Wait()
	close(acquired)

	var winners int
	var winnerID uint
	for o := range acquired {
		winners++
		winnerID = o.Occupancy.ID
	}
	if winners != 1 {
		t.Fatalf("acquired winners=%d, want exactly 1", winners)
	}

	// A live lease blocks a second claim.
	second, err := repo.Claim(ctx, constants.SMSKindGreeting, 7, 0, "daily-1-0", "", time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if second.Acquired {
		t.Fatal("second claim must not acquire while lease is live")
	}

	// Success closes the tuple permanently.
	if err := repo.MarkSuccess(ctx, winnerID, 99, now); err != nil {
		t.Fatal(err)
	}
	after, err := repo.Claim(ctx, constants.SMSKindGreeting, 7, 0, "daily-1-0", "", time.Minute, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if after.Acquired || after.Existing.Status != constants.OccupancySuccess || after.Existing.SuccessLogID == nil || *after.Existing.SuccessLogID != 99 {
		t.Fatalf("successful tuple must stay closed, got %+v", after.Existing)
	}
}

func TestSendOccupancyFailureAllowsRetry(t *testing.T) {
	db := newSerialDB(t)
	repo := NewSendOccupancyRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()

	first, err := repo.Claim(ctx, constants.SMSKindAlert, 3, 5, "", "confirm-100", time.Minute, now)
	if err != nil || !first.Acquired {
		t.Fatalf("first claim: %+v, %v", first, err)
	}
	if err := repo.MarkFailure(ctx, first.Occupancy.ID, "provider down", now); err != nil {
		t.Fatal(err)
	}

	// Failure does not close the event: a later scan reacquires it.
	retry, err := repo.Claim(ctx, constants.SMSKindAlert, 3, 5, "", "confirm-100", time.Minute, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Acquired || !retry.Reacquired || retry.Occupancy.Attempts != 2 {
		t.Fatalf("retry should reacquire failed tuple, got %+v", retry)
	}
}

func TestSendOccupancyExpiredLeaseReacquired(t *testing.T) {
	db := newSerialDB(t)
	repo := NewSendOccupancyRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()

	first, err := repo.Claim(ctx, constants.SMSKindGreeting, 9, 0, "weekly-2-1", "", time.Minute, now)
	if err != nil || !first.Acquired {
		t.Fatalf("first claim: %+v, %v", first, err)
	}
	// Simulate a crashed runner: lease expired while still processing.
	later := now.Add(2 * time.Minute)
	takeover, err := repo.Claim(ctx, constants.SMSKindGreeting, 9, 0, "weekly-2-1", "", time.Minute, later)
	if err != nil {
		t.Fatal(err)
	}
	if !takeover.Acquired || !takeover.Reacquired {
		t.Fatalf("expired lease should be reacquirable, got %+v", takeover)
	}
}

func TestSendOccupancyAbandonedDoesNotClose(t *testing.T) {
	db := newSerialDB(t)
	repo := NewSendOccupancyRepository(db)
	ctx := context.Background()
	now := time.Now().UTC()

	first, err := repo.Claim(ctx, constants.SMSKindAlert, 4, 1, "", "never-confirmed", time.Minute, now)
	if err != nil || !first.Acquired {
		t.Fatalf("first claim: %+v, %v", first, err)
	}
	if err := repo.MarkAbandoned(ctx, first.Occupancy.ID, "confirmed during scan", now); err != nil {
		t.Fatal(err)
	}
	// Abandoned must give up the event, not occupy it: a later claim succeeds.
	again, err := repo.Claim(ctx, constants.SMSKindAlert, 4, 1, "", "never-confirmed", time.Minute, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !again.Acquired {
		t.Fatalf("abandoned tuple must stay open, existing=%+v", again.Existing)
	}
}
