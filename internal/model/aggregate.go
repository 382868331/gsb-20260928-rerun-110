package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

const (
	IntervalUnpublished   = "unpublished"
	IntervalPublished     = "published"
	IntervalPendingReview = "pending_review"
)

// AggregateSpec defines a downstream time-bucketed statistic over current
// observation values. Measure is a base measure name (e.g. "temperature");
// contributions must agree on a single unit within one interval.
type AggregateSpec struct {
	ID         string        `json:"id"`
	Name       string        `json:"name"`
	Operator   string        `json:"operator"`
	IdentityID string        `json:"identity_id,omitempty"`
	Measure    string        `json:"measure"`
	From       time.Time     `json:"from"`
	To         time.Time     `json:"to"`
	Interval   time.Duration `json:"interval"`
	CreatedAt  time.Time     `json:"created_at"`
}

// Contribution traces one delivery's input to an interval statistic back to
// the original message, the interpretation source and the adoption decision.
type Contribution struct {
	DeliveryID  string       `json:"delivery_id"`
	RawRef      RawReference `json:"raw_ref"`
	Measure     string       `json:"measure"`
	Unit        string       `json:"unit,omitempty"`
	Value       float64      `json:"value"`
	Source      string       `json:"source"`
	BranchID    string       `json:"branch_id,omitempty"`
	AdoptionID  string       `json:"adoption_id,omitempty"`
	Fingerprint string       `json:"fingerprint"`
}

type SkippedContribution struct {
	DeliveryID string `json:"delivery_id"`
	Reason     string `json:"reason"`
}

type IntervalStats struct {
	Count int     `json:"count"`
	Sum   float64 `json:"sum"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Avg   float64 `json:"avg"`
}

// AggregateInterval is one immutable computed revision of an interval
// statistic. Recomputing after a correction appends a new revision; older
// revisions stay in the journal so published results remain inspectable.
type AggregateInterval struct {
	ID            string                `json:"id"`
	AggregateID   string                `json:"aggregate_id"`
	Start         time.Time             `json:"start"`
	End           time.Time             `json:"end"`
	Revision      int                   `json:"revision"`
	Stats         IntervalStats         `json:"stats"`
	Unit          string                `json:"unit,omitempty"`
	MixedUnits    bool                  `json:"mixed_units,omitempty"`
	Contributions []Contribution        `json:"contributions,omitempty"`
	Skipped       []SkippedContribution `json:"skipped,omitempty"`
	Fingerprint   string                `json:"fingerprint"`
	ComputedAt    time.Time             `json:"computed_at"`
}

// ContentFingerprint identifies the computed content of an interval so an
// interrupted run can resume without writing duplicate revisions.
func (i AggregateInterval) ContentFingerprint() string {
	payload := struct {
		Stats         IntervalStats         `json:"stats"`
		Unit          string                `json:"unit"`
		MixedUnits    bool                  `json:"mixed_units"`
		Contributions []Contribution        `json:"contributions"`
		Skipped       []SkippedContribution `json:"skipped"`
	}{i.Stats, i.Unit, i.MixedUnits, i.Contributions, i.Skipped}
	b, _ := json.Marshal(payload)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// AggregateSignoff publishes one interval revision. Sign-off is per
// interval, so a partial batch keeps the remaining intervals on their
// previously published revision with a pending note.
type AggregateSignoff struct {
	ID          string    `json:"id"`
	AggregateID string    `json:"aggregate_id"`
	IntervalID  string    `json:"interval_id"`
	Start       time.Time `json:"start"`
	Revision    int       `json:"revision"`
	Operator    string    `json:"operator"`
	Note        string    `json:"note,omitempty"`
	SignedAt    time.Time `json:"signed_at"`
}
