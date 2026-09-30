package promote

import (
	"strings"
	"testing"
)

func TestSelectLeavesOutHeliumRows(t *testing.T) {
	for _, spec := range Specs {
		exclude := heliumExclusionSQL(spec, "dev_az_"+spec.Name, "dev_az_helium_js_written")
		q := buildSelect("dev_az_"+spec.Name, []string{"asin"}, spec, 10, false, exclude)
		for _, want := range []string{"NOT EXISTS", `"dev_az_helium_js_written" w`, "w.table_name = '" + spec.Name + "'", `w.asin = "dev_az_` + spec.Name + `".asin`} {
			if !strings.Contains(q, want) {
				t.Errorf("%s: select lacks %q:\n%s", spec.Name, want, q)
			}
		}
		clock := "w.written_at +"
		if spec.NaiveClock {
			clock = "w.written_at_local +"
		}
		if !strings.Contains(q, clock) {
			t.Errorf("%s: wrong clock column:\n%s", spec.Name, q)
		}
	}
}

func TestSelectWithoutExclusionIsUnchanged(t *testing.T) {
	q := buildSelect("t", []string{"asin"}, Specs[0], 10, false, "")
	if strings.Contains(q, "NOT EXISTS") {
		t.Fatalf("unexpected exclusion: %s", q)
	}
}
