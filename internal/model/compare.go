package model

import (
	"reflect"
	"sort"
	"strings"
)

const (
	ChangeUnchanged  = "unchanged"
	ChangeValue      = "value_changed"
	ChangeUnit       = "unit_changed"
	ChangeAnnotation = "annotation_changed"
)

type MeasureDiff struct {
	Name     string  `json:"name"`
	OldName  string  `json:"old_name,omitempty"`
	Kind     string  `json:"kind"` // value | unit | added | removed
	OldValue float64 `json:"old_value,omitempty"`
	NewValue float64 `json:"new_value,omitempty"`
	OldUnit  string  `json:"old_unit,omitempty"`
	NewUnit  string  `json:"new_unit,omitempty"`
}

type Comparison struct {
	Change            string        `json:"change"`
	Measures          []MeasureDiff `json:"measures,omitempty"`
	AttributesChanged bool          `json:"attributes_changed,omitempty"`
}

var knownUnits = map[string]bool{
	"c": true, "f": true, "k": true, "pct": true,
	"g": true, "mg": true, "m": true, "mm": true,
	"s": true, "ms": true, "hz": true, "v": true,
	"a": true, "pa": true, "db": true, "rpm": true,
}

func splitMeasureName(name string) (base, unit string) {
	idx := strings.LastIndexByte(name, '_')
	if idx <= 0 || idx == len(name)-1 {
		return name, ""
	}
	suffix := name[idx+1:]
	if !knownUnits[suffix] {
		return name, ""
	}
	return name[:idx], suffix
}

// Compare classifies the difference between a previously confirmed
// observation and a reinterpretation of the same raw message.
func Compare(oldObs, newObs Observation) Comparison {
	cmp := Comparison{Change: ChangeUnchanged}
	if !reflect.DeepEqual(oldObs.Attributes, newObs.Attributes) {
		cmp.AttributesChanged = true
	}
	oldKeys := make([]string, 0, len(oldObs.Measures))
	for k := range oldObs.Measures {
		oldKeys = append(oldKeys, k)
	}
	sort.Strings(oldKeys)
	oldOnly := map[string]float64{}
	for _, k := range oldKeys {
		oldV := oldObs.Measures[k]
		if newV, ok := newObs.Measures[k]; ok {
			if oldV != newV {
				cmp.Measures = append(cmp.Measures, MeasureDiff{Name: k, Kind: "value", OldValue: oldV, NewValue: newV})
			}
			continue
		}
		oldOnly[k] = oldV
	}
	newOnly := map[string]float64{}
	for k, v := range newObs.Measures {
		if _, ok := oldObs.Measures[k]; !ok {
			newOnly[k] = v
		}
	}
	// Pair keys that differ only in unit suffix (e.g. temperature_c -> temperature_f).
	newKeys := make([]string, 0, len(newOnly))
	for k := range newOnly {
		newKeys = append(newKeys, k)
	}
	sort.Strings(newKeys)
	oldOrdered := make([]string, 0, len(oldOnly))
	for k := range oldOnly {
		oldOrdered = append(oldOrdered, k)
	}
	sort.Strings(oldOrdered)
	for _, oldK := range oldOrdered {
		oldV := oldOnly[oldK]
		oldBase, oldUnit := splitMeasureName(oldK)
		matched := ""
		for _, newK := range newKeys {
			if _, ok := newOnly[newK]; !ok {
				continue
			}
			newBase, newUnit := splitMeasureName(newK)
			if oldBase == newBase && oldUnit != newUnit {
				matched = newK
				break
			}
		}
		if matched != "" {
			_, newUnit := splitMeasureName(matched)
			cmp.Measures = append(cmp.Measures, MeasureDiff{
				Name: oldBase, OldName: oldK, Kind: "unit",
				OldValue: oldV, NewValue: newOnly[matched],
				OldUnit: oldUnit, NewUnit: newUnit,
			})
			delete(newOnly, matched)
			delete(oldOnly, oldK)
		}
	}
	for _, k := range newKeys {
		if v, ok := newOnly[k]; ok {
			cmp.Measures = append(cmp.Measures, MeasureDiff{Name: k, Kind: "added", NewValue: v})
		}
	}
	remainingOld := make([]string, 0, len(oldOnly))
	for k := range oldOnly {
		remainingOld = append(remainingOld, k)
	}
	sort.Strings(remainingOld)
	for _, k := range remainingOld {
		cmp.Measures = append(cmp.Measures, MeasureDiff{Name: k, Kind: "removed", OldValue: oldOnly[k]})
	}
	sort.SliceStable(cmp.Measures, func(i, j int) bool { return cmp.Measures[i].Name < cmp.Measures[j].Name })

	hasValue, hasUnit := false, false
	for _, d := range cmp.Measures {
		switch d.Kind {
		case "value", "added", "removed":
			hasValue = true
		case "unit":
			hasUnit = true
		}
	}
	switch {
	case hasValue:
		cmp.Change = ChangeValue
	case hasUnit:
		cmp.Change = ChangeUnit
	case cmp.AttributesChanged:
		cmp.Change = ChangeAnnotation
	default:
		cmp.Change = ChangeUnchanged
	}
	return cmp
}
