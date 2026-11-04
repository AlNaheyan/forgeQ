package backup

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

type Retention struct{ Daily, Weekly, MaxDelete int }

// PlanRetention selects newest verified snapshots per UTC day/week, always
// preserving the newest usable archive. Invalid/incomplete objects are retained
// for inspection. A manifest is removed before its archive, making interruption safe.
func PlanRetention(ctx context.Context, s Storage, prefix string, p Retention) ([]string, error) {
	if p.Daily < 1 || p.Weekly < 0 || p.MaxDelete < 1 || p.MaxDelete > 1000 {
		return nil, errors.New("invalid bounded retention")
	}
	keys, e := s.List(ctx)
	if e != nil {
		return nil, e
	}
	var usable []Manifest
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix+"/") || strings.Contains(strings.TrimPrefix(k, prefix+"/"), "/") {
			return nil, errors.New("storage returned foreign object")
		}
		if !strings.HasSuffix(k, ".age.json") {
			continue
		}
		m, e := ReadManifest(ctx, s, k)
		if e == nil && Verify(ctx, s, m) == nil {
			usable = append(usable, m)
		}
	}
	sort.Slice(usable, func(i, j int) bool { return usable[i].Snapshot.After(usable[j].Snapshot) })
	// Without a successful decrypt/restore receipt, retention is dry protection:
	// checksum validity alone cannot prove the key is recoverable.
	restoredObject := ""
	for _, m := range usable {
		if !restoredAt(ctx, s, m).IsZero() {
			restoredObject = m.Object
			break
		}
	}
	if restoredObject == "" {
		return nil, nil
	}
	days, weeks := map[string]bool{}, map[string]bool{}
	var plan []string
	for i, m := range usable {
		day := m.Snapshot.UTC().Format("2006-01-02")
		y, w := m.Snapshot.UTC().ISOWeek()
		week := fmtWeek(y, w)
		keep := i == 0 || m.Object == restoredObject
		if !days[day] && len(days) < p.Daily {
			days[day] = true
			keep = true
		}
		if !weeks[week] && len(weeks) < p.Weekly {
			weeks[week] = true
			keep = true
		}
		if !keep && len(plan)+2 <= p.MaxDelete {
			// Keep receipts permanently unless their archive itself is retained;
			// include exact receipt cleanup in the same bounded plan.
			if !restoredAt(ctx, s, m).IsZero() {
				if len(plan)+3 > p.MaxDelete {
					continue
				}
				plan = append(plan, m.Object+".restored.json")
			}
			plan = append(plan, m.Object+".json", m.Object)
		}
	}
	return plan, nil
}
func fmtWeek(y, w int) string { return fmt.Sprintf("%04d-%02d", y, w) }

// Retain recomputes its plan while caller holds the scheduler lock; apply=false
// is the safe default. No arbitrary caller-provided deletion list is accepted.
func Retain(ctx context.Context, s Storage, prefix string, p Retention, apply bool) ([]string, error) {
	plan, e := PlanRetention(ctx, s, prefix, p)
	if e != nil || !apply {
		return plan, e
	}
	for _, k := range plan {
		if e = s.Delete(ctx, k); e != nil {
			return plan, errors.New("retention deletion failed")
		}
	}
	return plan, nil
}
