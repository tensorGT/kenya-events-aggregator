package crawler

import "testing"

func TestExtractSiteMetadata(t *testing.T) {
	html := `<html><head>
		<title>Mookh Events Kenya</title>
		<meta name="description" content="Discover Kenyan events">
		<meta property="og:image" content="/images/logo.png">
	</head><body></body></html>`
	meta := ExtractSiteMetadata(html, "https://mookh.com/calendar")
	if meta.Title != "Mookh Events Kenya" {
		t.Fatalf("unexpected title: %q", meta.Title)
	}
	if meta.Description != "Discover Kenyan events" {
		t.Fatalf("unexpected description: %q", meta.Description)
	}
	if meta.LogoURL != "https://mookh.com/images/logo.png" {
		t.Fatalf("unexpected logo URL: %q", meta.LogoURL)
	}
}

func TestExtractJSONLDEventsAndRSC(t *testing.T) {
	rscHTML := `<html><script>self.__next_f.push([1,"{\"@context\":\"https://schema.org\",\"@type\":\"Event\",\"name\":\"Nairobi Afrobeats Night\",\"startDate\":\"2026-12-20T20:00:00+03:00\",\"location\":{\"@type\":\"Place\",\"name\":\"Alchemist, Westlands\",\"address\":{\"addressLocality\":\"Nairobi\"}},\"image\":[\"https://example.com/1.jpg\",\"https://example.com/2.jpg\"],\"offers\":[{\"name\":\"VIP\",\"price\":3000,\"priceCurrency\":\"KES\"}]}"])</script></html>`
	evs := ExtractJSONLDEvents(rscHTML, "https://ticketyetu.com/events/afrobeats")
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
	if evs[0].Title != "Nairobi Afrobeats Night" || len(evs[0].TicketTiers) != 1 || evs[0].TicketTiers[0].Price != 3000 {
		t.Fatalf("unexpected parsed event: %+v", evs[0])
	}
}

func TestParseGenericJSONEventsFullSchema(t *testing.T) {
	raw := `{
		"events": [{
			"title": "Diani Beach Fest",
			"description": "3-day coastal music festival",
			"venue": "Diani Reef Beach",
			"county": "Kwale",
			"main_photo": "https://cdn.example.com/main.jpg",
			"photos": ["https://cdn.example.com/p1.jpg", "https://cdn.example.com/p2.jpg", "https://cdn.example.com/p3.jpg", "https://cdn.example.com/p4.jpg", "https://cdn.example.com/p5.jpg"],
			"google_maps_link": "https://maps.google.com/?q=-4.28,39.59",
			"schedules": [
				{"date": "2026-12-18", "start_time": "16:00", "end_time": "23:59"},
				{"date": "2026-12-19", "start_time": "14:00", "end_time": "23:59"}
			],
			"ticket_tiers": [
				{"name": "Early Bird", "price": 2500, "currency": "KES", "conditions": "Valid before 6PM", "benefits": ["Welcome drink", "Beach access"]}
			],
			"itinerary": ["16:00 Gates Open", "20:00 Headliner"],
			"what_to_expect": ["Live DJs", "Food Stalls"]
		}]
	}`
	evs := parseGenericJSONEvents(raw, "https://custom.ke/api/events")
	if len(evs) != 1 {
		t.Fatalf("expected 1 event, got %d", len(evs))
	}
	ev := evs[0]
	if len(ev.Photos) != 4 {
		t.Fatalf("expected 4 extra photos max, got %d", len(ev.Photos))
	}
	if len(ev.Schedules) != 2 || len(ev.TicketTiers) != 1 || len(ev.Itinerary) != 2 || len(ev.WhatToExpect) != 2 {
		t.Fatalf("missing rich schema fields: %+v", ev)
	}
	if err := ev.Validate(); err != nil {
		t.Fatalf("validation failed: %v", err)
	}
	if ev.County != "Kwale" {
		t.Fatalf("expected county Kwale, got %q", ev.County)
	}
}
