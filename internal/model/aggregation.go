package model

import "time"

const (
	AggregationWindowBaseline = "baseline"
	AggregationWindowSigned   = "signed"
	AggregationWindowPending  = "pending"
	AggregationWindowRestore  = "restore_available"

	AggregationRevisionBaseline = "baseline"
	AggregationRevisionDraft    = "draft"
	AggregationRevisionPartial  = "partially_signed"
	AggregationRevisionSigned   = "signed"
)

type AggregationWindow struct {
	ID         string    `json:"id"`
	IdentityID string    `json:"identity_id,omitempty"`
	BootID     string    `json:"boot_id,omitempty"`
	From       time.Time `json:"from"`
	To         time.Time `json:"to"`
}

func (w AggregationWindow) Contains(value CurrentValue) bool {
	at := value.Observation.ObservedAt
	if at.IsZero() {
		at = value.Observation.ReceivedAt
	}
	if w.IdentityID != "" && value.Observation.IdentityID != w.IdentityID {
		return false
	}
	if w.BootID != "" && value.RawRef.BootID != w.BootID {
		return false
	}
	if !w.From.IsZero() && at.Before(w.From) {
		return false
	}
	if !w.To.IsZero() && at.After(w.To) {
		return false
	}
	return true
}

type Aggregation struct {
	ID        string              `json:"id"`
	Name      string              `json:"name"`
	Operator  string              `json:"operator,omitempty"`
	Windows   []AggregationWindow `json:"windows"`
	CreatedAt time.Time           `json:"created_at"`
}

type AggregationValue struct {
	DeliveryID  string       `json:"delivery_id"`
	RawRef      RawReference `json:"raw_ref"`
	Observation Observation  `json:"observation"`
	Source      string       `json:"source"`
	BranchID    string       `json:"branch_id,omitempty"`
	Rule        string       `json:"rule,omitempty"`
	AdoptionID  string       `json:"adoption_id,omitempty"`
	Fingerprint string       `json:"fingerprint"`
}

type AggregateMeasure struct {
	Count int     `json:"count"`
	Sum   float64 `json:"sum"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Avg   float64 `json:"avg"`
}

type AggregationWindowRevision struct {
	Window             AggregationWindow           `json:"window"`
	Status             string                      `json:"status"`
	Values             []AggregationValue          `json:"values"`
	Statistics         map[string]AggregateMeasure `json:"statistics,omitempty"`
	BaselineStatistics map[string]AggregateMeasure `json:"baseline_statistics,omitempty"`
	ChangedDeliveryIDs []string                    `json:"changed_delivery_ids,omitempty"`
	PendingDeliveryIDs []string                    `json:"pending_delivery_ids,omitempty"`
	Reason             string                      `json:"reason,omitempty"`
	Signoffs           []AggregationSignoff        `json:"signoffs,omitempty"`
}

type AggregationRevision struct {
	ID            string                      `json:"id"`
	AggregationID string                      `json:"aggregation_id"`
	BranchID      string                      `json:"branch_id,omitempty"`
	Rule          string                      `json:"rule,omitempty"`
	Operator      string                      `json:"operator,omitempty"`
	Reason        string                      `json:"reason,omitempty"`
	Status        string                      `json:"status"`
	Windows       []AggregationWindowRevision `json:"windows"`
	CreatedAt     time.Time                   `json:"created_at"`
}

type AggregationSignoff struct {
	ID            string    `json:"id"`
	AggregationID string    `json:"aggregation_id"`
	RevisionID    string    `json:"revision_id"`
	WindowID      string    `json:"window_id"`
	Operator      string    `json:"operator"`
	Reason        string    `json:"reason,omitempty"`
	SignedAt      time.Time `json:"signed_at"`
}

type AggregationWindowResult struct {
	Window      AggregationWindow           `json:"window"`
	Status      string                      `json:"status"`
	RevisionID  string                      `json:"revision_id,omitempty"`
	Statistics  map[string]AggregateMeasure `json:"statistics"`
	Values      []AggregationValue          `json:"values"`
	Reason      string                      `json:"reason,omitempty"`
	RestoreFrom []AggregationValue          `json:"restore_from,omitempty"`
}

type AggregationResult struct {
	Aggregation Aggregation               `json:"aggregation"`
	Windows     []AggregationWindowResult `json:"windows"`
}
