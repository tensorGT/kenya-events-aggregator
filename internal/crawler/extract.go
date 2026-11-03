package crawler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/cladder/ke-events/internal/domain"
	"github.com/cladder/ke-events/internal/resilience"
)

var httpClient = &http.Client{
	Timeout: 15 * time.Second,
}

type SiteMetadata struct {
	Title       string
	Description string
	LogoURL     string
}

func FetchURL(ctx context.Context, rawURL string) (string, string, error) {
	if err := resilience.ValidateExternalURL(rawURL); err != nil {
		return "", "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/json,application/xml;q=0.9,*/*;q=0.8")

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", fmt.Errorf("HTTP %d from %s", resp.StatusCode, rawURL)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return "", "", err
	}
	return string(body), resp.Header.Get("Content-Type"), nil
}

func ExtractSiteMetadata(htmlBody, baseURL string) SiteMetadata {
	var meta SiteMetadata
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlBody))
	if err != nil {
		return meta
	}
	meta.Title = strings.TrimSpace(doc.Find("title").First().Text())
	if d, ok := doc.Find(`meta[name="description"]`).Attr("content"); ok {
		meta.Description = strings.TrimSpace(d)
	} else if d, ok := doc.Find(`meta[property="og:description"]`).Attr("content"); ok {
		meta.Description = strings.TrimSpace(d)
	}

	var logo string
	if og, ok := doc.Find(`meta[property="og:image"]`).Attr("content"); ok && og != "" {
		logo = og
	} else if ic, ok := doc.Find(`link[rel*="icon"]`).Attr("href"); ok && ic != "" {
		logo = ic
	}
	if logo != "" {
		meta.LogoURL = resolveURL(baseURL, logo)
	}
	return meta
}

func resolveURL(base, ref string) string {
	bu, err := url.Parse(base)
	if err != nil {
		return ref
	}
	ru, err := url.Parse(strings.TrimSpace(ref))
	if err != nil {
		return ref
	}
	return bu.ResolveReference(ru).String()
}

var locRegex = regexp.MustCompile(`<loc>\s*([^<]+?)\s*</loc>`)

func ExtractSitemapURLs(xmlBody string, includeSubstr string, limit int) []string {
	matches := locRegex.FindAllStringSubmatch(xmlBody, -1)
	var out []string
	seen := make(map[string]struct{})
	for _, m := range matches {
		u := strings.TrimSpace(m[1])
		if includeSubstr != "" && !strings.Contains(strings.ToLower(u), strings.ToLower(includeSubstr)) {
			continue
		}
		if _, ok := seen[u]; ok {
			continue
		}
		seen[u] = struct{}{}
		out = append(out, u)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// ExtractJSONLDEvents parses Schema.org Event objects from HTML <script type="application/ld+json"> tags.
func ExtractJSONLDEvents(htmlBody, pageURL string) []domain.CanonicalEvent {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlBody))
	if err != nil {
		return nil
	}

	var events []domain.CanonicalEvent
	doc.Find(`script[type="application/ld+json"]`).Each(func(_ int, s *goquery.Selection) {
		raw := strings.TrimSpace(s.Text())
		if raw == "" {
			return
		}
		for _, obj := range flattenJSONLD(raw) {
			if ev, ok := mapSchemaEvent(obj, pageURL); ok {
				events = append(events, ev)
			}
		}
	})

	// Also check Next.js RSC escaped JSON-LD if standard script tags yielded none
	if len(events) == 0 && (strings.Contains(htmlBody, `"@type":"Event"`) || strings.Contains(htmlBody, `\"@type\":\"Event\"`)) {
		unesc := strings.ReplaceAll(htmlBody, `\"`, `"`)
		for _, rawObj := range extractBraceBalancedJSON(unesc, `"@type":"Event"`, 5) {
			var obj map[string]any
			if json.Unmarshal([]byte(rawObj), &obj) == nil {
				if ev, ok := mapSchemaEvent(obj, pageURL); ok {
					events = append(events, ev)
				}
			}
		}
	}

	return events
}

func extractBraceBalancedJSON(text, marker string, limit int) []string {
	var out []string
	offset := 0
	for len(out) < limit {
		idx := strings.Index(text[offset:], marker)
		if idx == -1 {
			break
		}
		absIdx := offset + idx
		// Walk backward to the opening '{' of this JSON object
		start := strings.LastIndexByte(text[:absIdx], '{')
		if start == -1 {
			offset = absIdx + len(marker)
			continue
		}
		depth := 0
		inStr := false
		escaped := false
		end := -1
		for i := start; i < len(text) && i < start+16000; i++ {
			c := text[i]
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' && inStr {
				escaped = true
				continue
			}
			if c == '"' {
				inStr = !inStr
				continue
			}
			if inStr {
				continue
			}
			if c == '{' {
				depth++
			} else if c == '}' {
				depth--
				if depth == 0 {
					end = i + 1
					break
				}
			}
		}
		if end > start {
			out = append(out, text[start:end])
			offset = end
		} else {
			offset = absIdx + len(marker)
		}
	}
	return out
}

func flattenJSONLD(raw string) []map[string]any {
	var out []map[string]any
	var single map[string]any
	if json.Unmarshal([]byte(raw), &single) == nil {
		if graph, ok := single["@graph"].([]any); ok {
			for _, g := range graph {
				if m, ok := g.(map[string]any); ok {
					out = append(out, m)
				}
			}
		} else {
			out = append(out, single)
		}
		return out
	}
	var arr []map[string]any
	if json.Unmarshal([]byte(raw), &arr) == nil {
		out = append(out, arr...)
	}
	return out
}

func mapSchemaEvent(obj map[string]any, pageURL string) (domain.CanonicalEvent, bool) {
	tStr := fmt.Sprintf("%v", obj["@type"])
	if !strings.Contains(tStr, "Event") {
		return domain.CanonicalEvent{}, false
	}
	title := strVal(obj["name"])
	if title == "" {
		return domain.CanonicalEvent{}, false
	}

	desc := strVal(obj["description"])
	if desc == "" {
		desc = title
	}

	venue, county, mapsLink := parseLocation(obj["location"])
	if venue == "" {
		venue = "Nairobi, Kenya"
	}

	mainPhoto, extraPhotos := parseImages(obj["image"], pageURL)
	if mainPhoto == "" {
		return domain.CanonicalEvent{}, false
	}

	schedules := parseSchedules(strVal(obj["startDate"]), strVal(obj["endDate"]))
	if len(schedules) == 0 {
		return domain.CanonicalEvent{}, false
	}

	tiers := parseOffers(obj["offers"])
	evURL := strVal(obj["url"])
	if evURL == "" {
		evURL = pageURL
	} else {
		evURL = resolveURL(pageURL, evURL)
	}

	return domain.CanonicalEvent{
		ExternalID:     evURL,
		Title:          title,
		Description:    desc,
		Venue:          venue,
		County:         county,
		EventURL:       evURL,
		GoogleMapsLink: mapsLink,
		MainPhotoURL:   mainPhoto,
		Photos:         extraPhotos,
		Schedules:      schedules,
		TicketTiers:    tiers,
		RawPayload:     obj,
	}, true
}

func parseLocation(locAny any) (venue, county, mapsLink string) {
	m, ok := locAny.(map[string]any)
	if !ok {
		s := strVal(locAny)
		return s, s, ""
	}
	venue = strVal(m["name"])
	if addr, ok := m["address"].(map[string]any); ok {
		loc := strVal(addr["addressLocality"])
		reg := strVal(addr["addressRegion"])
		street := strVal(addr["streetAddress"])
		if venue == "" {
			venue = street
		}
		county = strings.TrimSpace(loc + " " + reg)
	} else if addrStr := strVal(m["address"]); addrStr != "" {
		county = addrStr
	}
	if geo, ok := m["geo"].(map[string]any); ok {
		lat := strVal(geo["latitude"])
		lng := strVal(geo["longitude"])
		if lat != "" && lng != "" {
			mapsLink = fmt.Sprintf("https://maps.google.com/?q=%s,%s", lat, lng)
		}
	}
	return venue, county, mapsLink
}

func parseImages(imgAny any, baseURL string) (string, []string) {
	var all []string
	switch v := imgAny.(type) {
	case string:
		if v != "" {
			all = append(all, resolveURL(baseURL, v))
		}
	case []any:
		for _, item := range v {
			if s := strVal(item); s != "" {
				all = append(all, resolveURL(baseURL, s))
			} else if m, ok := item.(map[string]any); ok {
				if u := strVal(m["url"]); u != "" {
					all = append(all, resolveURL(baseURL, u))
				}
			}
		}
	case map[string]any:
		if u := strVal(v["url"]); u != "" {
			all = append(all, resolveURL(baseURL, u))
		}
	}
	if len(all) == 0 {
		return "", nil
	}
	main := all[0]
	var extra []string
	if len(all) > 1 {
		limit := len(all)
		if limit > 5 {
			limit = 5
		}
		extra = all[1:limit]
	}
	return main, extra
}

func parseSchedules(startStr, endStr string) []domain.EventSchedule {
	if startStr == "" {
		return nil
	}
	datePart := startStr
	timePart := "18:00"
	if len(startStr) >= 10 {
		datePart = startStr[:10]
	}
	if len(startStr) >= 16 && (startStr[10] == 'T' || startStr[10] == ' ') {
		timePart = startStr[11:16]
	}
	endTime := ""
	if len(endStr) >= 16 && (endStr[10] == 'T' || endStr[10] == ' ') {
		endTime = endStr[11:16]
	}
	return []domain.EventSchedule{{
		Date:      datePart,
		StartTime: timePart,
		EndTime:   endTime,
		Timezone:  "Africa/Nairobi",
	}}
}

func parseOffers(offersAny any) []domain.TicketTier {
	var rawList []map[string]any
	switch v := offersAny.(type) {
	case map[string]any:
		rawList = append(rawList, v)
	case []any:
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				rawList = append(rawList, m)
			}
		}
	}

	var tiers []domain.TicketTier
	for _, m := range rawList {
		name := strVal(m["name"])
		if name == "" {
			name = "General Admission"
		}
		price, _ := strconv.ParseFloat(strVal(m["price"]), 64)
		cur := strVal(m["priceCurrency"])
		if cur == "" {
			cur = "KES"
		}
		avail := strVal(m["availability"])
		if strings.Contains(avail, "SoldOut") {
			avail = "SoldOut"
		} else {
			avail = "InStock"
		}
		tiers = append(tiers, domain.TicketTier{
			Name:         name,
			Price:        price,
			Currency:     cur,
			Availability: avail,
		})
	}
	return tiers
}

func strVal(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case int:
		return strconv.Itoa(x)
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}
