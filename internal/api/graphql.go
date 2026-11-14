package api

import (
	"encoding/json"
	"net/http"

	"github.com/cladder/ke-events/internal/db"
	"github.com/cladder/ke-events/internal/domain"
	"github.com/graphql-go/graphql"
)

func NewGraphQLHandler(store *db.Store) (http.HandlerFunc, error) {
	tierType := graphql.NewObject(graphql.ObjectConfig{
		Name: "TicketTier",
		Fields: graphql.Fields{
			"name":         &graphql.Field{Type: graphql.String},
			"price":        &graphql.Field{Type: graphql.Float},
			"currency":     &graphql.Field{Type: graphql.String},
			"conditions":   &graphql.Field{Type: graphql.String},
			"benefits":     &graphql.Field{Type: graphql.NewList(graphql.String)},
			"availability": &graphql.Field{Type: graphql.String},
		},
	})

	schedType := graphql.NewObject(graphql.ObjectConfig{
		Name: "EventSchedule",
		Fields: graphql.Fields{
			"date":       &graphql.Field{Type: graphql.String},
			"start_time": &graphql.Field{Type: graphql.String},
			"end_time":   &graphql.Field{Type: graphql.String},
			"timezone":   &graphql.Field{Type: graphql.String},
		},
	})

	eventType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Event",
		Fields: graphql.Fields{
			"id":                     &graphql.Field{Type: graphql.String},
			"short_code":             &graphql.Field{Type: graphql.String},
			"source_name":            &graphql.Field{Type: graphql.String},
			"title":                  &graphql.Field{Type: graphql.String},
			"description":            &graphql.Field{Type: graphql.String},
			"venue":                  &graphql.Field{Type: graphql.String},
			"county":                 &graphql.Field{Type: graphql.String},
			"event_url":              &graphql.Field{Type: graphql.String},
			"google_maps_link":       &graphql.Field{Type: graphql.String},
			"main_photo":             &graphql.Field{Type: graphql.String, Resolve: func(p graphql.ResolveParams) (any, error) { return p.Source.(domain.CanonicalEvent).MainPhotoURL, nil }},
			"main_photo_storage_url": &graphql.Field{Type: graphql.String},
			"photos":                 &graphql.Field{Type: graphql.NewList(graphql.String)},
			"itinerary":              &graphql.Field{Type: graphql.NewList(graphql.String)},
			"what_to_expect":         &graphql.Field{Type: graphql.NewList(graphql.String)},
			"schedules":              &graphql.Field{Type: graphql.NewList(schedType)},
			"ticket_tiers":           &graphql.Field{Type: graphql.NewList(tierType)},
		},
	})

	sourceType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Source",
		Fields: graphql.Fields{
			"id":               &graphql.Field{Type: graphql.String},
			"slug":             &graphql.Field{Type: graphql.String},
			"name":             &graphql.Field{Type: graphql.String},
			"domain":           &graphql.Field{Type: graphql.String},
			"site_title":       &graphql.Field{Type: graphql.String},
			"site_description": &graphql.Field{Type: graphql.String},
			"logo_url":         &graphql.Field{Type: graphql.String},
			"circuit_state":    &graphql.Field{Type: graphql.String},
		},
	})

	rootQuery := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"events": &graphql.Field{
				Type: graphql.NewList(eventType),
				Args: graphql.FieldConfigArgument{
					"county": &graphql.ArgumentConfig{Type: graphql.String},
					"query":  &graphql.ArgumentConfig{Type: graphql.String},
					"limit":  &graphql.ArgumentConfig{Type: graphql.Int, DefaultValue: 20},
					"offset": &graphql.ArgumentConfig{Type: graphql.Int, DefaultValue: 0},
				},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					county, _ := p.Args["county"].(string)
					q, _ := p.Args["query"].(string)
					limit, _ := p.Args["limit"].(int)
					offset, _ := p.Args["offset"].(int)
					evs, _, err := store.ListEvents(p.Context, db.EventFilter{
						County: county, Query: q, Limit: limit, Offset: offset,
					})
					return evs, err
				},
			},
			"event": &graphql.Field{
				Type: eventType,
				Args: graphql.FieldConfigArgument{
					"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					ev, err := store.GetEvent(p.Context, p.Args["id"].(string))
					if err != nil {
						return nil, err
					}
					return *ev, nil
				},
			},
			"sources": &graphql.Field{
				Type: graphql.NewList(sourceType),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					return store.ListSources(p.Context)
				},
			},
			"counties": &graphql.Field{
				Type: graphql.NewList(graphql.String),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					return domain.KenyanCounties, nil
				},
			},
		},
	})

	schema, err := graphql.NewSchema(graphql.SchemaConfig{Query: rootQuery})
	if err != nil {
		return nil, err
	}

	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid graphql payload"})
			return
		}
		res := graphql.Do(graphql.Params{
			Schema:         schema,
			RequestString:  req.Query,
			VariableValues: req.Variables,
			Context:        r.Context(),
		})
		writeJSON(w, http.StatusOK, res)
	}, nil
}
