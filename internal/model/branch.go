package model

import "time"

const (
	BranchActive    = "active"
	BranchAdopted   = "adopted"
	BranchAbandoned = "abandoned"

	EntryIncluded = "included"
	EntryAdopted  = "adopted"
	EntryExcluded = "excluded"

	ReferenceValid      = "valid"
	ReferenceReview     = "needs_review"
	ReferenceIncomplete = "incomplete"
)

// BranchScope selects messages by device, receive-time range and sequence
// range. Every zero boundary is unrestricted.
type BranchScope struct {
	IdentityID   string    `json:"identity_id,omitempty"`
	BootID       string    `json:"boot_id,omitempty"`
	FromSequence int64     `json:"from_sequence,omitempty"`
	ToSequence   int64     `json:"to_sequence,omitempty"`
	From         time.Time `json:"from,omitempty"`
	To           time.Time `json:"to,omitempty"`
}

func (s BranchScope) Matches(identityID, bootID string, sequence int64, receivedAt time.Time) bool {
	if s.IdentityID != "" && identityID != s.IdentityID {
		return false
	}
	if s.BootID != "" && bootID != s.BootID {
		return false
	}
	if s.FromSequence != 0 && sequence < s.FromSequence {
		return false
	}
	if s.ToSequence != 0 && sequence > s.ToSequence {
		return false
	}
	if !s.From.IsZero() && receivedAt.Before(s.From) {
		return false
	}
	if !s.To.IsZero() && receivedAt.After(s.To) {
		return false
	}
	return true
}

type CorrectionBranch struct {
	ID            string      `json:"id"`
	Rule          string      `json:"rule"`
	Operator      string      `json:"operator"`
	Reason        string      `json:"reason,omitempty"`
	AbandonReason string      `json:"abandon_reason,omitempty"`
	Scope         BranchScope `json:"scope"`
	Status        string      `json:"status"`
	CreatedAt     time.Time   `json:"created_at"`
	UpdatedAt     time.Time   `json:"updated_at"`
}

type BranchEntry struct {
	ID          string       `json:"id"`
	BranchID    string       `json:"branch_id"`
	Rule        string       `json:"rule"`
	RawRef      RawReference `json:"raw_ref"`
	Observation Observation  `json:"observation"`
	Comparison  Comparison   `json:"comparison"`
	Status      string       `json:"status"`
	Reason      string       `json:"reason,omitempty"`
	CreatedAt   time.Time    `json:"created_at"`
}

type Adoption struct {
	ID          string            `json:"id"`
	BranchID    string            `json:"branch_id"`
	Rule        string            `json:"rule"`
	Operator    string            `json:"operator"`
	Reason      string            `json:"reason,omitempty"`
	Scope       BranchScope       `json:"scope"`
	DeliveryIDs []string          `json:"delivery_ids"`
	References  []ReferenceImpact `json:"references,omitempty"`
	Withdrawn   []string          `json:"withdrawn_deliveries,omitempty"`
	AdoptedAt   time.Time         `json:"adopted_at"`
}

type AdoptionWithdrawal struct {
	ID          string    `json:"id"`
	AdoptionID  string    `json:"adoption_id"`
	BranchID    string    `json:"branch_id"`
	Operator    string    `json:"operator"`
	Reason      string    `json:"reason"`
	DeliveryIDs []string  `json:"delivery_ids"`
	WithdrawnAt time.Time `json:"withdrawn_at"`
}

type BranchConflict struct {
	BranchID         string    `json:"branch_id"`
	ExistingBranchID string    `json:"existing_branch_id"`
	DeliveryIDs      []string  `json:"delivery_ids"`
	Reason           string    `json:"reason"`
	RecordedAt       time.Time `json:"recorded_at"`
}

type DownstreamReference struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Operator    string            `json:"operator,omitempty"`
	DeliveryIDs []string          `json:"delivery_ids"`
	Baseline    map[string]string `json:"baseline,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
}

type ReferenceImpact struct {
	ReferenceID string   `json:"reference_id"`
	Name        string   `json:"name"`
	Status      string   `json:"status"`
	DeliveryIDs []string `json:"delivery_ids,omitempty"`
	Reason      string   `json:"reason,omitempty"`
}

type CurrentValue struct {
	DeliveryID       string       `json:"delivery_id"`
	RawRef           RawReference `json:"raw_ref"`
	Observation      Observation  `json:"observation"`
	Source           string       `json:"source"` // original | branch
	BranchID         string       `json:"branch_id,omitempty"`
	Rule             string       `json:"rule,omitempty"`
	AdoptionID       string       `json:"adoption_id,omitempty"`
	AdoptedAt        time.Time    `json:"adopted_at,omitempty"`
	Withdrawn        bool         `json:"withdrawn,omitempty"`
	WithdrawalID     string       `json:"withdrawal_id,omitempty"`
	WithdrawnAt      time.Time    `json:"withdrawn_at,omitempty"`
	WithdrawalReason string       `json:"withdrawal_reason,omitempty"`
}
