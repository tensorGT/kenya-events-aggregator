package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

// KenyanCounties holds all 47 official counties of Kenya.
var KenyanCounties = []string{
	"Baringo", "Bomet", "Bungoma", "Busia", "Elgeyo-Marakwet",
	"Embu", "Garissa", "Homa Bay", "Isiolo", "Kajiado",
	"Kakamega", "Kericho", "Kiambu", "Kilifi", "Kirinyaga",
	"Kisii", "Kisumu", "Kitui", "Kwale", "Laikipia",
	"Lamu", "Machakos", "Makueni", "Mandera", "Marsabit",
	"Meru", "Migori", "Mombasa", "Murang'a", "Nairobi",
	"Nakuru", "Nandi", "Narok", "Nyamira", "Nyandarua",
	"Nyeri", "Samburu", "Siaya", "Taita-Taveta", "Tana River",
	"Tharaka-Nithi", "Trans Nzoia", "Turkana", "Uasin Gishu", "Vihiga",
	"Wajir", "West Pokot",
}

var countyAliases = map[string]string{
	"nairobi":          "Nairobi",
	"nairobi city":     "Nairobi",
	"westlands":        "Nairobi",
	"karen":            "Nairobi",
	"kilimani":         "Nairobi",
	"kicc":             "Nairobi",
	"mombasa":          "Mombasa",
	"nyali":            "Mombasa",
	"diani":            "Kwale",
	"ukunda":           "Kwale",
	"kilifi":           "Kilifi",
	"watamu":           "Kilifi",
	"malindi":          "Kilifi",
	"kisumu":           "Kisumu",
	"nakuru":           "Nakuru",
	"naivasha":         "Nakuru",
	"eldoret":          "Uasin Gishu",
	"uasin gishu":      "Uasin Gishu",
	"kiambu":           "Kiambu",
	"thika":            "Kiambu",
	"ruiru":            "Kiambu",
	"limuru":           "Kiambu",
	"machakos":         "Machakos",
	"athi river":       "Machakos",
	"kajiado":          "Kajiado",
	"ngong":            "Kajiado",
	"kitengela":        "Kajiado",
	"ongata rongai":    "Kajiado",
	"muranga":          "Murang'a",
	"murang'a":         "Murang'a",
	"nyeri":            "Nyeri",
	"nanyuki":          "Laikipia",
	"laikipia":         "Laikipia",
	"kisii":            "Kisii",
	"kakamega":         "Kakamega",
	"meru":             "Meru",
	"embu":             "Embu",
	"nyamira":          "Nyamira",
	"kericho":          "Kericho",
	"narok":            "Narok",
	"maasai mara":      "Narok",
	"lamu":             "Lamu",
	"taita taveta":     "Taita-Taveta",
	"taita-taveta":     "Taita-Taveta",
	"voi":              "Taita-Taveta",
	"kitale":           "Trans Nzoia",
	"trans nzoia":      "Trans Nzoia",
	"bungoma":          "Bungoma",
	"homa bay":         "Homa Bay",
	"migori":           "Migori",
	"siaya":            "Siaya",
	"busia":            "Busia",
	"bomet":            "Bomet",
	"nandi":            "Nandi",
	"baringo":          "Baringo",
	"elgeyo marakwet":  "Elgeyo-Marakwet",
	"elgeyo-marakwet":  "Elgeyo-Marakwet",
	"kirinyaga":        "Kirinyaga",
	"nyandarua":        "Nyandarua",
	"tharaka nithi":    "Tharaka-Nithi",
	"tharaka-nithi":    "Tharaka-Nithi",
	"kitui":            "Kitui",
	"makueni":          "Makueni",
	"garissa":          "Garissa",
	"wajir":            "Wajir",
	"mandera":          "Mandera",
	"marsabit":         "Marsabit",
	"isiolo":           "Isiolo",
	"samburu":          "Samburu",
	"turkana":          "Turkana",
	"west pokot":       "West Pokot",
	"vihiga":           "Vihiga",
	"tana river":       "Tana River",
}

// NormalizeCounty maps a raw locality, address, or venue string to one of the 47 Kenyan counties.
func NormalizeCounty(rawCounty, venue string) (string, bool) {
	combined := strings.ToLower(strings.TrimSpace(rawCounty + " " + venue))
	for _, c := range KenyanCounties {
		if strings.Contains(combined, strings.ToLower(c)) {
			return c, true
		}
	}
	for alias, canonical := range countyAliases {
		if strings.Contains(combined, alias) {
			return canonical, true
		}
	}
	return "", false
}

// TicketTier represents a pricing tier and its conditions/benefits.
type TicketTier struct {
	Name         string   `json:"name"`
	Price        float64  `json:"price"`
	Currency     string   `json:"currency"`
	Conditions   string   `json:"conditions,omitempty"`
	Benefits     []string `json:"benefits,omitempty"`
	Availability string   `json:"availability,omitempty"`
}

// EventSchedule represents a single date and time window for an event.
type EventSchedule struct {
	Date      string `json:"date"`       // YYYY-MM-DD
	StartTime string `json:"start_time"` // HH:MM
	EndTime   string `json:"end_time,omitempty"`
	Timezone  string `json:"timezone,omitempty"`
}

// Source represents a Kenyan event ticketer endpoint and its site metadata.
type Source struct {
	ID                  uuid.UUID  `json:"id"`
	Slug                string     `json:"slug"`
	Name                string     `json:"name"`
	Domain              string     `json:"domain"`
	EndpointURL         string     `json:"endpoint_url"`
	AdapterType         string     `json:"adapter_type"`
	SiteTitle           string     `json:"site_title"`
	SiteDescription     string     `json:"site_description"`
	LogoURL             string     `json:"logo_url"`
	LogoStorageURL      string     `json:"logo_storage_url,omitempty"`
	IsActive            bool       `json:"is_active"`
	CircuitState        string     `json:"circuit_state"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	LastCrawledAt       *time.Time `json:"last_crawled_at,omitempty"`
	LastError           string     `json:"last_error,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
}

// CanonicalEvent is the normalized event model across all Kenyan ticketers.
type CanonicalEvent struct {
	ID                   uuid.UUID       `json:"id"`
	SourceID             uuid.UUID       `json:"source_id"`
	SourceName           string          `json:"source_name,omitempty"`
	ExternalID           string          `json:"external_id"`
	ContentHash          string          `json:"content_hash"`
	ShortCode            string          `json:"short_code"`
	Title                string          `json:"title"`
	Description          string          `json:"description"`
	Venue                string          `json:"venue"`
	County               string          `json:"county"`
	EventURL             string          `json:"event_url,omitempty"`
	GoogleMapsLink       string          `json:"google_maps_link,omitempty"`
	MainPhotoURL         string          `json:"main_photo"`
	MainPhotoStorageURL  string          `json:"main_photo_storage_url,omitempty"`
	Photos               []string        `json:"photos,omitempty"`
	Schedules            []EventSchedule `json:"schedules"`
	TicketTiers          []TicketTier    `json:"ticket_tiers"`
	Itinerary            []string        `json:"itinerary,omitempty"`
	WhatToExpect         []string        `json:"what_to_expect,omitempty"`
	RawPayload           map[string]any  `json:"-"`
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
}

// Validate enforces all required fields and Kenyan county constraints.
func (e *CanonicalEvent) Validate() error {
	if strings.TrimSpace(e.Title) == "" {
		return errors.New("event title is required")
	}
	if strings.TrimSpace(e.Description) == "" {
		e.Description = e.Title
	}
	if strings.TrimSpace(e.Venue) == "" {
		return errors.New("venue is required")
	}
	county, ok := NormalizeCounty(e.County, e.Venue)
	if !ok {
		return fmt.Errorf("county %q is not a recognized Kenyan county", e.County)
	}
	e.County = county

	if strings.TrimSpace(e.MainPhotoURL) == "" {
		return errors.New("main_photo is required")
	}
	if len(e.Photos) > 4 {
		e.Photos = e.Photos[:4]
	}
	if len(e.Schedules) == 0 {
		return errors.New("at least one event date/time schedule is required")
	}
	if e.GoogleMapsLink != "" {
		if u, err := url.ParseRequestURI(e.GoogleMapsLink); err != nil || (u.Scheme != "http" && u.Scheme != "https") {
			e.GoogleMapsLink = ""
		}
	}
	if e.TicketTiers == nil {
		e.TicketTiers = []TicketTier{}
	}
	e.ComputeHashes()
	return nil
}

// ComputeHashes calculates the deduplication content_hash and Base62 short_code.
func (e *CanonicalEvent) ComputeHashes() {
	dateKey := ""
	if len(e.Schedules) > 0 {
		dateKey = e.Schedules[0].Date
	}
	norm := strings.ToLower(strings.TrimSpace(e.Title)) + "|" +
		strings.ToLower(strings.TrimSpace(e.Venue)) + "|" +
		strings.ToLower(strings.TrimSpace(e.County)) + "|" + dateKey
	sum := sha256.Sum256([]byte(norm))
	e.ContentHash = hex.EncodeToString(sum[:])

	if e.ShortCode == "" {
		srcSum := sha256.Sum256([]byte(e.ExternalID + "|" + e.ContentHash))
		e.ShortCode = encodeBase62(srcSum[:6])
	}
}

const base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func encodeBase62(b []byte) string {
	var val uint64
	for _, c := range b {
		val = (val << 8) | uint64(c)
	}
	if val == 0 {
		return "0"
	}
	var out [12]byte
	i := len(out)
	for val > 0 && i > 0 {
		i--
		out[i] = base62Alphabet[val%62]
		val /= 62
	}
	return string(out[i:])
}
