package constants

const (
	RecipientStatusActive = "Active"
	RecipientStatusPaused = "Paused"

	FrequencyDaily   = "daily"
	FrequencyWeekly  = "weekly"
	FrequencyMonthly = "monthly"

	SMSResultSuccess = "success"
	SMSResultFailed  = "failed"

	SMSKindGreeting = "greeting"
	SMSKindAlert    = "alert"

	// Send occupancy lifecycle states.
	OccupancyProcessing = "processing"
	OccupancySuccess    = "success"
	OccupancyFailed     = "failed"
	OccupancyAbandoned  = "abandoned"
)
