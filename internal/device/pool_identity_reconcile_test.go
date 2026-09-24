package device

import "testing"

func TestSimIdentityKnownIMMatchesNormalizedValues(t *testing.T) {
	known := []string{"515661015796311", "234159610965314"}
	if !simIdentityKnownIMSI(known, "515661015796311") {
		t.Fatal("exact IMSI should be recognised")
	}
	if !simIdentityKnownIMSI(known, " 515661015796311 ") {
		t.Fatal("padded IMSI should be recognised")
	}
	if simIdentityKnownIMSI(known, "204047616796311") {
		t.Fatal("unrelated IMSI must not match")
	}
	if simIdentityKnownIMSI(known, "") {
		t.Fatal("empty IMSI must not match")
	}
	if simIdentityKnownIMSI(nil, "515661015796311") {
		t.Fatal("empty history must not match")
	}
}
