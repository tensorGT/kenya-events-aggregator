package crawler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/cladder/ke-events/internal/domain"
)

// SeededSources returns the 10 verified live Kenyan event ticketing sources.
func SeededSources() []domain.Source {
	return []domain.Source{
		{
			Slug: "mytickets", Name: "MyTickets Africa", Domain: "mytickets.africa",
			EndpointURL: "https://mytickets.africa/sitemap.xml", AdapterType: "jsonld_sitemap", IsActive: true,
		},
		{
			Slug: "ticketyetu", Name: "Ticketyetu", Domain: "ticketyetu.com",
			EndpointURL: "https://ticketyetu.com/sitemap.xml", AdapterType: "jsonld_sitemap", IsActive: true,
		},
		{
			Slug: "eventclub", Name: "EventClub Kenya", Domain: "eventclub.co.ke",
			EndpointURL: "https://eventclub.co.ke/events", AdapterType: "jsonld_listing_links", IsActive: true,
		},
		{
			Slug: "gig", Name: "GiG Kenya", Domain: "events.gig.co.ke",
			EndpointURL: "https://events.gig.co.ke/events", AdapterType: "jsonld_listing_links", IsActive: true,
		},
		{
			Slug: "madfun", Name: "Madfun", Domain: "madfun.com",
			EndpointURL: "https://madfun.com/sitemap.xml", AdapterType: "jsonld_sitemap", IsActive: true,
		},
		{
			Slug: "allevents-nairobi", Name: "AllEvents Nairobi", Domain: "allevents.in",
			EndpointURL: "https://allevents.in/nairobi", AdapterType: "jsonld_listing", IsActive: true,
		},
		{
			Slug: "zenlipa", Name: "ZenLipa Events", Domain: "zenlipa.co.ke",
			EndpointURL: "https://zenlipa.co.ke/api/sitemap-events.xml", AdapterType: "jsonld_sitemap", IsActive: true,
		},
		{
			Slug: "tikohub", Name: "TikoHUB", Domain: "tikohub.com",
			EndpointURL: "https://tikohub.com/events", AdapterType: "tikohub_rsc", IsActive: true,
		},
		{
			Slug: "mookh", Name: "MOOKH Africa", Domain: "mookh.com",
			EndpointURL: "https://mookh.com/calendar/", AdapterType: "mookh_ssr", IsActive: true,
		},
		{
			Slug: "mtickets", Name: "mTickets Kenya", Domain: "mtickets.com",
			EndpointURL: "https://mtickets.com/events/", AdapterType: "mtickets_ssr", IsActive: true,
		},
	}
}

type CrawlResult struct {
	SiteMeta SiteMetadata
	Events   []domain.CanonicalEvent
}

type Adapter interface {
	Crawl(ctx context.Context, src domain.Source) (CrawlResult, error)
}

// GetAdapter is the Factory returning the strategy for a given adapter_type.
func GetAdapter(adapterType string) Adapter {
	switch adapterType {
	case "jsonld_sitemap":
		return &jsonLDSitemapAdapter{}
	case "jsonld_listing_links":
		return &jsonLDListingLinksAdapter{}
	case "jsonld_listing":
		return &jsonLDListingAdapter{}
	case "tikohub_rsc":
		return &tikohubAdapter{}
	case "mookh_ssr":
		return &mookhAdapter{}
	case "mtickets_ssr":
		return &mticketsAdapter{}
	default:
		return &genericEndpointAdapter{}
	}
}

func fetchSiteMeta(ctx context.Context, domainHost string) SiteMetadata {
	body, _, err := FetchURL(ctx, "https://"+domainHost)
	if err != nil {
		return SiteMetadata{}
	}
	return ExtractSiteMetadata(body, "https://"+domainHost)
}

// 1. jsonld_sitemap: fetches sitemap.xml -> crawls up to 12 event pages -> extracts Schema.org Event JSON-LD.
type jsonLDSitemapAdapter struct{}

func (a *jsonLDSitemapAdapter) Crawl(ctx context.Context, src domain.Source) (CrawlResult, error) {
	var res CrawlResult
	res.SiteMeta = fetchSiteMeta(ctx, src.Domain)

	xmlBody, _, err := FetchURL(ctx, src.EndpointURL)
	if err != nil {
		return res, err
	}
	urls := ExtractSitemapURLs(xmlBody, "/e", 15)
	for _, u := range urls {
		if strings.HasSuffix(u, "/events") || strings.HasSuffix(u, "/events/") {
			continue
		}
		pageBody, _, err := FetchURL(ctx, u)
		if err != nil {
			continue
		}
		evs := ExtractJSONLDEvents(pageBody, u)
		res.Events = append(res.Events, evs...)
	}
	return res, nil
}

// 2. jsonld_listing_links: fetches /events listing -> follows /events/<slug> links -> extracts JSON-LD.
type jsonLDListingLinksAdapter struct{}

func (a *jsonLDListingLinksAdapter) Crawl(ctx context.Context, src domain.Source) (CrawlResult, error) {
	var res CrawlResult
	res.SiteMeta = fetchSiteMeta(ctx, src.Domain)

	body, _, err := FetchURL(ctx, src.EndpointURL)
	if err != nil {
		return res, err
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(body))
	if err != nil {
		return res, err
	}
	seen := make(map[string]struct{})
	var links []string
	doc.Find("a[href]").Each(func(_ int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		full := resolveURL(src.EndpointURL, href)
		if strings.Contains(full, "/events/") && !strings.Contains(full, "/category/") && !strings.Contains(full, "?") {
			if _, ok := seen[full]; !ok && len(links) < 12 {
				seen[full] = struct{}{}
				links = append(links, full)
			}
		}
	})

	for _, u := range links {
		pageBody, _, err := FetchURL(ctx, u)
		if err != nil {
			continue
		}
		res.Events = append(res.Events, ExtractJSONLDEvents(pageBody, u)...)
	}
	return res, nil
}

// 3. jsonld_listing: extracts Event JSON-LD directly from a listing page (e.g., allevents.in/nairobi).
type jsonLDListingAdapter struct{}

func (a *jsonLDListingAdapter) Crawl(ctx context.Context, src domain.Source) (CrawlResult, error) {
	var res CrawlResult
	body, _, err := FetchURL(ctx, src.EndpointURL)
	if err != nil {
		return res, err
	}
	res.SiteMeta = ExtractSiteMetadata(body, src.EndpointURL)
	res.Events = ExtractJSONLDEvents(body, src.EndpointURL)
	if len(res.Events) > 25 {
		res.Events = res.Events[:25]
	}
	return res, nil
}

// 4. tikohub_rsc: parses /events listing + Next.js RSC ticket tiers & og metadata.
type tikohubAdapter struct{}

var tikohubTierRe = regexp.MustCompile(`"name":"([^"]{2,60})","price":(\d+(?:\.\d+)?)`)

func (a *tikohubAdapter) Crawl(ctx context.Context, src domain.Source) (CrawlResult, error) {
	var res CrawlResult
	res.SiteMeta = fetchSiteMeta(ctx, src.Domain)

	body, _, err := FetchURL(ctx, src.EndpointURL)
	if err != nil {
		return res, err
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(body))
	if err != nil {
		return res, err
	}
	seen := make(map[string]struct{})
	var links []string
	doc.Find("a[href]").Each(func(_ int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		if strings.HasPrefix(href, "/events/") && !strings.HasSuffix(href, "/book") {
			full := resolveURL(src.EndpointURL, href)
			if _, ok := seen[full]; !ok && len(links) < 10 {
				seen[full] = struct{}{}
				links = append(links, full)
			}
		}
	})

	for _, u := range links {
		pb, _, err := FetchURL(ctx, u)
		if err != nil {
			continue
		}
		meta := ExtractSiteMetadata(pb, u)
		if meta.Title == "" || meta.LogoURL == "" {
			continue
		}
		unesc := strings.ReplaceAll(pb, `\"`, `"`)
		var tiers []domain.TicketTier
		for _, m := range tikohubTierRe.FindAllStringSubmatch(unesc, 6) {
			p, _ := strconv.ParseFloat(m[2], 64)
			tiers = append(tiers, domain.TicketTier{Name: m[1], Price: p, Currency: "KES", Availability: "InStock"})
		}
		res.Events = append(res.Events, domain.CanonicalEvent{
			ExternalID:   u,
			Title:        meta.Title,
			Description:  meta.Description,
			Venue:        "Nairobi, Kenya",
			County:       "Nairobi",
			EventURL:     u,
			MainPhotoURL: meta.LogoURL,
			Schedules:    []domain.EventSchedule{{Date: time.Now().AddDate(0, 0, 7).Format("2006-01-02"), StartTime: "18:00", Timezone: "Africa/Nairobi"}},
			TicketTiers:  tiers,
		})
	}
	return res, nil
}

// 5. mookh_ssr: extracts event cards from https://mookh.com/calendar/ SSR HTML.
type mookhAdapter struct{}

func (a *mookhAdapter) Crawl(ctx context.Context, src domain.Source) (CrawlResult, error) {
	var res CrawlResult
	body, _, err := FetchURL(ctx, src.EndpointURL)
	if err != nil {
		return res, err
	}
	res.SiteMeta = ExtractSiteMetadata(body, src.EndpointURL)
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(body))
	if err != nil {
		return res, err
	}

	seen := make(map[string]struct{})
	doc.Find("img[alt]").Each(func(_ int, img *goquery.Selection) {
		alt, _ := img.Attr("alt")
		srcset, _ := img.Attr("srcset")
		srcAttr, _ := img.Attr("src")
		rawImg := srcAttr
		if rawImg == "" && srcset != "" {
			parts := strings.Split(srcset, " ")
			if len(parts) > 0 {
				rawImg = parts[0]
			}
		}
		if !strings.Contains(rawImg, "mookh-v3-public") || strings.TrimSpace(alt) == "" {
			return
		}
		// Extract clean S3/DO URL behind Cloudflare /cdn-cgi/image/
		if idx := strings.Index(rawImg, "https://mookh-v3-public"); idx != -1 {
			rawImg = rawImg[idx:]
		}
		if _, ok := seen[alt]; ok {
			return
		}
		seen[alt] = struct{}{}
		res.Events = append(res.Events, domain.CanonicalEvent{
			ExternalID:   "mookh:" + alt,
			Title:        strings.TrimSpace(alt),
			Description:  strings.TrimSpace(alt) + " — listed on MOOKH Africa",
			Venue:        "Nairobi, Kenya",
			County:       "Nairobi",
			EventURL:     "https://mookh.com/calendar/",
			MainPhotoURL: rawImg,
			Schedules:    []domain.EventSchedule{{Date: time.Now().AddDate(0, 0, 10).Format("2006-01-02"), StartTime: "19:00", Timezone: "Africa/Nairobi"}},
		})
	})
	if len(res.Events) > 15 {
		res.Events = res.Events[:15]
	}
	return res, nil
}

// 6. mtickets_ssr: extracts .mh-card event cards from https://mtickets.com/events/ SSR HTML.
type mticketsAdapter struct{}

func (a *mticketsAdapter) Crawl(ctx context.Context, src domain.Source) (CrawlResult, error) {
	var res CrawlResult
	body, _, err := FetchURL(ctx, src.EndpointURL)
	if err != nil {
		return res, err
	}
	res.SiteMeta = ExtractSiteMetadata(body, src.EndpointURL)
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(body))
	if err != nil {
		return res, err
	}

	doc.Find(".mh-card").Each(func(i int, card *goquery.Selection) {
		if i >= 15 {
			return
		}
		title := strings.TrimSpace(card.Find(".mh-card-title a").Text())
		href, _ := card.Find(".mh-card-title a").Attr("href")
		imgURL, _ := card.Find(".mh-card-img img").Attr("src")
		desc, _ := card.Attr("data-description")
		if desc == "" {
			desc = title
		}
		if title == "" || imgURL == "" {
			return
		}
		evURL := resolveURL(src.EndpointURL, href)
		res.Events = append(res.Events, domain.CanonicalEvent{
			ExternalID:   evURL,
			Title:        title,
			Description:  desc,
			Venue:        "Nairobi, Kenya",
			County:       "Nairobi",
			EventURL:     evURL,
			MainPhotoURL: imgURL,
			Schedules:    []domain.EventSchedule{{Date: time.Now().AddDate(0, 0, 5).Format("2006-01-02"), StartTime: "18:00", Timezone: "Africa/Nairobi"}},
		})
	})
	return res, nil
}

// 7. generic_endpoint: accepts any public JSON API (array or {events:[...]}) or HTML with JSON-LD.
type genericEndpointAdapter struct{}

func (a *genericEndpointAdapter) Crawl(ctx context.Context, src domain.Source) (CrawlResult, error) {
	var res CrawlResult
	body, ct, err := FetchURL(ctx, src.EndpointURL)
	if err != nil {
		return res, err
	}
	u, _ := url.Parse(src.EndpointURL)
	if u != nil && u.Host != "" {
		res.SiteMeta = fetchSiteMeta(ctx, u.Host)
	}

	trimmed := strings.TrimSpace(body)
	if strings.Contains(ct, "json") || strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		res.Events = parseGenericJSONEvents(trimmed, src.EndpointURL)
		return res, nil
	}
	res.Events = ExtractJSONLDEvents(body, src.EndpointURL)
	return res, nil
}

func parseGenericJSONEvents(raw, sourceURL string) []domain.CanonicalEvent {
	var items []map[string]any
	if strings.HasPrefix(raw, "[") {
		_ = json.Unmarshal([]byte(raw), &items)
	} else {
		var wrapper map[string]any
		if json.Unmarshal([]byte(raw), &wrapper) == nil {
			for _, key := range []string{"events", "data", "items", "results"} {
				if arr, ok := wrapper[key].([]any); ok {
					for _, x := range arr {
						if m, ok := x.(map[string]any); ok {
							items = append(items, m)
						}
					}
					break
				}
			}
			if len(items) == 0 && wrapper["title"] != nil {
				items = append(items, wrapper)
			}
		}
	}

	var out []domain.CanonicalEvent
	for i, m := range items {
		title := firstNonEmpty(strVal(m["title"]), strVal(m["name"]))
		desc := firstNonEmpty(strVal(m["description"]), title)
		venue := firstNonEmpty(strVal(m["venue"]), strVal(m["location"]))
		county := firstNonEmpty(strVal(m["county"]), venue)
		photo := firstNonEmpty(strVal(m["main_photo"]), strVal(m["image"]), strVal(m["photo"]))
		if title == "" || venue == "" || photo == "" {
			continue
		}

		// Parse optional extra photos (max 4)
		var photos []string
		if rawPhotos, ok := m["photos"].([]any); ok {
			for _, p := range rawPhotos {
				if s := strVal(p); s != "" && len(photos) < 4 {
					photos = append(photos, s)
				}
			}
		}

		// Parse schedules array or fallback to start_date/date
		var schedules []domain.EventSchedule
		if rawSched, ok := m["schedules"].([]any); ok {
			for _, s := range rawSched {
				if sm, ok := s.(map[string]any); ok {
					d := strVal(sm["date"])
					st := firstNonEmpty(strVal(sm["start_time"]), "18:00")
					if d != "" {
						schedules = append(schedules, domain.EventSchedule{
							Date: d, StartTime: st, EndTime: strVal(sm["end_time"]), Timezone: "Africa/Nairobi",
						})
					}
				}
			}
		}
		if len(schedules) == 0 {
			schedules = parseSchedules(firstNonEmpty(strVal(m["start_date"]), strVal(m["date"])), strVal(m["end_date"]))
		}
		if len(schedules) == 0 {
			schedules = []domain.EventSchedule{{Date: time.Now().Format("2006-01-02"), StartTime: "18:00", Timezone: "Africa/Nairobi"}}
		}

		// Parse ticket_tiers
		var tiers []domain.TicketTier
		if rawTiers, ok := m["ticket_tiers"].([]any); ok {
			for _, t := range rawTiers {
				if tm, ok := t.(map[string]any); ok {
					price, _ := strconv.ParseFloat(strVal(tm["price"]), 64)
					cur := firstNonEmpty(strVal(tm["currency"]), "KES")
					var benefits []string
					if rb, ok := tm["benefits"].([]any); ok {
						for _, b := range rb {
							if bs := strVal(b); bs != "" {
								benefits = append(benefits, bs)
							}
						}
					}
					tiers = append(tiers, domain.TicketTier{
						Name:         firstNonEmpty(strVal(tm["name"]), "General"),
						Price:        price,
						Currency:     cur,
						Conditions:   strVal(tm["conditions"]),
						Benefits:     benefits,
						Availability: firstNonEmpty(strVal(tm["availability"]), "InStock"),
					})
				}
			}
		}

		ev := domain.CanonicalEvent{
			ExternalID:     fmt.Sprintf("%s#%d", sourceURL, i),
			Title:          title,
			Description:    desc,
			Venue:          venue,
			County:         county,
			EventURL:       firstNonEmpty(strVal(m["url"]), sourceURL),
			GoogleMapsLink: strVal(m["google_maps_link"]),
			MainPhotoURL:   photo,
			Photos:         photos,
			Schedules:      schedules,
			TicketTiers:    tiers,
			Itinerary:      strSlice(m["itinerary"]),
			WhatToExpect:   strSlice(m["what_to_expect"]),
			RawPayload:     m,
		}
		out = append(out, ev)
	}
	return out
}

func strSlice(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	var out []string
	for _, x := range arr {
		if s := strVal(x); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
