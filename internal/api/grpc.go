package api

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"

	eventsv1 "github.com/cladder/ke-events/gen/events/v1"
	"github.com/cladder/ke-events/internal/broker"
	"github.com/cladder/ke-events/internal/cache"
	"github.com/cladder/ke-events/internal/crawler"
	"github.com/cladder/ke-events/internal/db"
	"github.com/cladder/ke-events/internal/domain"
	"github.com/cladder/ke-events/internal/resilience"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCServer struct {
	eventsv1.UnimplementedEventAggregationServiceServer
	Store  *db.Store
	Cache  *cache.Client
	Broker *broker.Client
}

func (g *GRPCServer) GetEvent(ctx context.Context, req *eventsv1.GetEventRequest) (*eventsv1.GetEventResponse, error) {
	ev, err := g.Store.GetEvent(ctx, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "event not found: %v", err)
	}
	return &eventsv1.GetEventResponse{Event: toProtoEvent(*ev)}, nil
}

func (g *GRPCServer) ListEvents(ctx context.Context, req *eventsv1.ListEventsRequest) (*eventsv1.ListEventsResponse, error) {
	evs, total, err := g.Store.ListEvents(ctx, db.EventFilter{
		County: req.County,
		Query:  req.Query,
		Limit:  int(req.Limit),
		Offset: int(req.Offset),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list events: %v", err)
	}
	out := make([]*eventsv1.EventMessage, 0, len(evs))
	for _, ev := range evs {
		out = append(out, toProtoEvent(ev))
	}
	return &eventsv1.ListEventsResponse{Events: out, Total: total}, nil
}

func (g *GRPCServer) RegisterSource(ctx context.Context, req *eventsv1.RegisterSourceRequest) (*eventsv1.RegisterSourceResponse, error) {
	if err := resilience.ValidateExternalURL(req.EndpointUrl); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid endpoint url: %v", err)
	}
	u, _ := url.Parse(req.EndpointUrl)
	slug := strings.ReplaceAll(strings.ToLower(u.Host), ".", "-")
	adapter := req.AdapterType
	if adapter == "" {
		adapter = "generic_endpoint"
	}
	src := &domain.Source{
		Slug:        slug,
		Name:        req.Name,
		Domain:      u.Host,
		EndpointURL: req.EndpointUrl,
		AdapterType: adapter,
		IsActive:    true,
	}
	if body, _, err := crawler.FetchURL(ctx, "https://"+u.Host); err == nil {
		meta := crawler.ExtractSiteMetadata(body, "https://"+u.Host)
		src.SiteTitle = meta.Title
		src.SiteDescription = meta.Description
		src.LogoURL = meta.LogoURL
	}
	if err := g.Store.UpsertSource(ctx, src); err != nil {
		return nil, status.Errorf(codes.Internal, "save source: %v", err)
	}
	if g.Broker != nil {
		payload, _ := json.Marshal(src)
		_ = g.Broker.Publish(ctx, broker.TopicCrawlJobs, src.Slug, payload)
	}
	return &eventsv1.RegisterSourceResponse{
		SourceId:    src.ID.String(),
		Title:       src.SiteTitle,
		Description: src.SiteDescription,
		LogoUrl:     src.LogoURL,
		Status:      "queued",
	}, nil
}

func (g *GRPCServer) IngestEndpoint(ctx context.Context, req *eventsv1.IngestEndpointRequest) (*eventsv1.IngestEndpointResponse, error) {
	if err := resilience.ValidateExternalURL(req.EndpointUrl); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "ssrf/url error: %v", err)
	}
	u, _ := url.Parse(req.EndpointUrl)
	src := &domain.Source{
		Slug:        "custom-" + strings.ReplaceAll(strings.ToLower(u.Host), ".", "-"),
		Name:        u.Host,
		Domain:      u.Host,
		EndpointURL: req.EndpointUrl,
		AdapterType: "generic_endpoint",
		IsActive:    true,
	}
	_ = g.Store.UpsertSource(ctx, src)

	res, err := crawler.GetAdapter(src.AdapterType).Crawl(ctx, *src)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "crawl endpoint: %v", err)
	}
	var ids []string
	for i := range res.Events {
		res.Events[i].SourceID = src.ID
		if g.Store.UpsertEventTx(ctx, &res.Events[i]) == nil {
			ids = append(ids, res.Events[i].ID.String())
		}
	}
	g.Cache.InvalidatePrefix(ctx, "events:")
	return &eventsv1.IngestEndpointResponse{
		IngestedCount: int32(len(ids)),
		EventIds:      ids,
	}, nil
}

func (g *GRPCServer) StreamEvents(req *eventsv1.StreamEventsRequest, stream eventsv1.EventAggregationService_StreamEventsServer) error {
	sub := g.Cache.SubscribeEvents(stream.Context())
	defer sub.Close()

	ch := sub.Channel()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			var ev domain.CanonicalEvent
			if json.Unmarshal([]byte(msg.Payload), &ev) != nil {
				continue
			}
			if req.County != "" && !strings.EqualFold(req.County, ev.County) {
				continue
			}
			if err := stream.Send(toProtoEvent(ev)); err != nil {
				return err
			}
		}
	}
}

func toProtoEvent(ev domain.CanonicalEvent) *eventsv1.EventMessage {
	msg := &eventsv1.EventMessage{
		Id:                   ev.ID.String(),
		ShortCode:            ev.ShortCode,
		SourceId:             ev.SourceID.String(),
		SourceName:           ev.SourceName,
		Title:                ev.Title,
		Description:          ev.Description,
		Venue:                ev.Venue,
		County:               ev.County,
		MainPhoto:            ev.MainPhotoURL,
		MainPhotoStorageUrl:  ev.MainPhotoStorageURL,
		Photos:               ev.Photos,
		GoogleMapsLink:       ev.GoogleMapsLink,
		Itinerary:            ev.Itinerary,
		WhatToExpect:         ev.WhatToExpect,
		EventUrl:             ev.EventURL,
		CreatedAt:            ev.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
	for _, s := range ev.Schedules {
		msg.Schedules = append(msg.Schedules, &eventsv1.EventSchedule{
			Date: s.Date, StartTime: s.StartTime, EndTime: s.EndTime,
		})
	}
	for _, t := range ev.TicketTiers {
		msg.TicketTiers = append(msg.TicketTiers, &eventsv1.TicketTier{
			Name: t.Name, Price: t.Price, Currency: t.Currency, Conditions: t.Conditions, Benefits: t.Benefits, Availability: t.Availability,
		})
	}
	return msg
}
