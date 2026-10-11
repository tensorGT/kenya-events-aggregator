package domain

import "testing"

func TestNormalizeCounty(t *testing.T) {
	cases := []struct {
		raw   string
		venue string
		want  string
		ok    bool
	}{
		{"Nairobi", "KICC", "Nairobi", true},
		{"", "Diani Beach Resort", "Kwale", true},
		{"", "Sarova Woodlands, Nakuru", "Nakuru", true},
		{"", "Naivasha Kongoni", "Nakuru", true},
		{"", "Unknown Mars Venue", "", false},
	}
	for _, tc := range cases {
		got, ok := NormalizeCounty(tc.raw, tc.venue)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("NormalizeCounty(%q, %q) = (%q, %v), want (%q, %v)", tc.raw, tc.venue, got, ok, tc.want, tc.ok)
		}
	}
}

func TestCanonicalEventValidate(t *testing.T) {
	ev := CanonicalEvent{
		ExternalID:   "ext-1",
		Title:        "Nairobi Jazz Festival",
		Description:  "Live jazz in Karen",
		Venue:        "Carnivore Grounds",
		County:       "Nairobi",
		MainPhotoURL: "https://example.com/poster.jpg",
		Photos:       []string{"1", "2", "3", "4", "5", "6"},
		Schedules:    []EventSchedule{{Date: "2026-12-12", StartTime: "18:00"}},
	}
	if err := ev.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ev.Photos) != 4 {
		t.Fatalf("expected photos capped at 4, got %d", len(ev.Photos))
	}
	if ev.ShortCode == "" || ev.ContentHash == "" {
		t.Fatal("expected short_code and content_hash to be populated")
	}
}
