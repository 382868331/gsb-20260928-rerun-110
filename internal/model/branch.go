package model

import "time"

// Correction branch lifecycle states.
const (
	BranchOpen             = "open"
	BranchAdopted          = "adopted"
	BranchPartiallyAdopted = "partially_adopted"
	BranchAbandoned        = "abandoned"
)

// BranchFilter selects the device identity and receive-time range a
// correction branch applies to. Zero values mean no restriction.
type BranchFilter struct {
	IdentityID string    `json:"identity_id,omitempty"`
	From       time.Time `json:"from,omitempty"`
	To         time.Time `json:"to,omitempty"`
}

// CorrectionBranch is an alternative interpretation rule proposed by an
// operator over a device/time scope. The original observation stays
// untouched; the branch only becomes effective through an Adoption.
type CorrectionBranch struct {
	ID           string       `json:"id"`
	Rule         string       `json:"rule"`
	Filter       BranchFilter `json:"filter"`
	Operator     string       `json:"operator"`
	Reason       string       `json:"reason"`
	Status       string       `json:"status"`
	BaseRevision int64        `json:"base_revision"`
	CreatedAt    time.Time    `json:"created_at"`
	ClosedAt     time.Time    `json:"closed_at,omitempty"`
	CloseReason  string       `json:"close_reason,omitempty"`
}

// AdoptionEntry records one delivery adopted under a correction branch:
// the new observation, its comparison against the previously effective
// interpretation, and a pointer to what it superseded.
type AdoptionEntry struct {
	RawRef             RawReference `json:"raw_ref"`
	Observation        Observation  `json:"observation"`
	Comparison         Comparison   `json:"comparison"`
	PreviousRule       string       `json:"previous_rule"`
	PreviousAdoptionID string       `json:"previous_adoption_id,omitempty"`
}

// Adoption is the formal record that a branch (or a narrowed part of it)
// became the effective interpretation for a set of deliveries.
type Adoption struct {
	ID        string          `json:"id"`
	BranchID  string          `json:"branch_id"`
	Rule      string          `json:"rule"`
	Operator  string          `json:"operator"`
	Revision  int64           `json:"revision"`
	Entries   []AdoptionEntry `json:"entries"`
	CreatedAt time.Time       `json:"created_at"`
}

// Retraction formally withdraws a subset of an adoption's deliveries.
// Affected deliveries revert to the interpretation that preceded it.
type Retraction struct {
	ID          string    `json:"id"`
	AdoptionID  string    `json:"adoption_id"`
	Operator    string    `json:"operator"`
	Reason      string    `json:"reason"`
	DeliveryIDs []string  `json:"delivery_ids"`
	Revision    int64     `json:"revision"`
	CreatedAt   time.Time `json:"created_at"`
}

// DownstreamRef registers that an external consumer referenced the
// effective interpretation (Rule, "" = original) of a delivery, so later
// adoptions can report which references stay valid and which need review.
type DownstreamRef struct {
	ID         string    `json:"id"`
	DeliveryID string    `json:"delivery_id"`
	Rule       string    `json:"rule"`
	Note       string    `json:"note,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// CurrentValue is the effective interpretation of one delivery together
// with its full provenance: the stored raw message and the adoption
// decision (empty = original ingest interpretation) that produced it.
type CurrentValue struct {
	DeliveryID  string       `json:"delivery_id"`
	Observation Observation  `json:"observation"`
	Rule        string       `json:"rule"`
	AdoptionID  string       `json:"adoption_id,omitempty"`
	RawRef      RawReference `json:"raw_ref"`
}
