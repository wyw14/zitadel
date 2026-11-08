// Package upcaster_test contains the hermetic tests of the eventstore
// upcaster registry. It lives in a subpackage on purpose: its own test binary
// does not inherit the eventstore package's TestMain, which requires an
// embedded PostgreSQL instance. Everything under test is exercised through
// exported APIs with a fake querier.
package upcaster_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/zitadel/zitadel/internal/api/authz"
	"github.com/zitadel/zitadel/internal/database"
	"github.com/zitadel/zitadel/internal/eventstore"
)

// upcastTestEnvelope is a stand-in for a repository.Event envelope.
type upcastTestEnvelope struct {
	typ      eventstore.EventType
	agg      *eventstore.Aggregate
	seq      uint64
	pos      decimal.Decimal
	inTx     uint32
	creation time.Time
	creator  string
	data     []byte
}

func newUpcastEnvelope(aggregateType eventstore.AggregateType, eventType eventstore.EventType, revision uint16, seq uint64, data []byte) *upcastTestEnvelope {
	return &upcastTestEnvelope{
		typ: eventType,
		agg: &eventstore.Aggregate{
			ID:            "aggregate-1",
			Type:          aggregateType,
			ResourceOwner: "org-1",
			InstanceID:    "instance-1",
			Version:       eventstore.Version(fmt.Sprintf("v%d", revision)),
		},
		seq:      seq,
		pos:      decimal.NewFromInt(1000 + int64(seq)),
		inTx:     uint32(seq),
		creation: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC),
		creator:  "user-1",
		data:     data,
	}
}

func (e *upcastTestEnvelope) Aggregate() *eventstore.Aggregate { return e.agg }
func (e *upcastTestEnvelope) Creator() string                  { return e.creator }
func (e *upcastTestEnvelope) Type() eventstore.EventType       { return e.typ }
func (e *upcastTestEnvelope) Revision() uint16                 { return parseTestRevision(e.agg.Version) }
func (e *upcastTestEnvelope) Sequence() uint64                 { return e.seq }
func (e *upcastTestEnvelope) CreatedAt() time.Time             { return e.creation }
func (e *upcastTestEnvelope) Position() decimal.Decimal        { return e.pos }
func (e *upcastTestEnvelope) InTxOrder() uint32                { return e.inTx }
func (e *upcastTestEnvelope) DataAsBytes() []byte              { return e.data }
func (e *upcastTestEnvelope) Unmarshal(ptr any) error {
	if len(e.data) == 0 {
		return nil
	}
	return json.Unmarshal(e.data, ptr)
}

func parseTestRevision(v eventstore.Version) uint16 {
	var revision uint16
	if _, err := fmt.Sscanf(string(v), "v%d", &revision); err != nil {
		return 0
	}
	return revision
}

// upcastFakeQuerier replays a fixed batch of envelopes through a reducer.
type upcastFakeQuerier struct {
	events []eventstore.Event
}

func (q *upcastFakeQuerier) Health(context.Context) error { return nil }
func (q *upcastFakeQuerier) FilterToReducer(_ context.Context, _ *eventstore.SearchQueryBuilder, r eventstore.Reducer) error {
	for _, event := range q.events {
		if err := r(event); err != nil {
			return err
		}
	}
	return nil
}
func (q *upcastFakeQuerier) LatestPosition(context.Context, *eventstore.SearchQueryBuilder) (decimal.Decimal, error) {
	return decimal.Decimal{}, nil
}
func (q *upcastFakeQuerier) InstanceIDs(context.Context, *eventstore.SearchQueryBuilder) ([]string, error) {
	return nil, nil
}
func (q *upcastFakeQuerier) Client() *database.DB { return nil }

// ------------------------------------------------------------
// Three-generation payload fixtures
// ------------------------------------------------------------

const (
	upcastAggregateType eventstore.AggregateType = "upcast.test.aggregate"
	upcastEventType     eventstore.EventType     = "upcast.profile.updated"
)

type upcastPayloadV1 struct {
	Name string `json:"name"`
}
type upcastPayloadV2 struct {
	DisplayName string `json:"displayName"`
}
type upcastPayloadV3 struct {
	DisplayName string `json:"displayName"`
	Active      bool   `json:"active"`
}

func upcastV1ToV2(in *eventstore.UpcastInput) ([]byte, error) {
	var v1 upcastPayloadV1
	if err := json.Unmarshal(in.Payload, &v1); err != nil {
		return nil, err
	}
	return json.Marshal(upcastPayloadV2{DisplayName: v1.Name})
}

func upcastV2ToV3(in *eventstore.UpcastInput) ([]byte, error) {
	var v2 upcastPayloadV2
	if err := json.Unmarshal(in.Payload, &v2); err != nil {
		return nil, err
	}
	return json.Marshal(upcastPayloadV3{DisplayName: v2.DisplayName, Active: true})
}

type upcastProfileEvent struct {
	eventstore.BaseEvent `json:"-"`

	DisplayName string `json:"displayName"`
	Active      bool   `json:"active"`
}

func upcastProfileMapper(event eventstore.Event) (eventstore.Event, error) {
	mapped := &upcastProfileEvent{BaseEvent: *eventstore.BaseEventFromRepo(event)}
	if err := event.Unmarshal(mapped); err != nil {
		return nil, err
	}
	return mapped, nil
}

type upcastProfileReducer struct {
	names   []string
	actives []bool
	pending []eventstore.Event
}

func (r *upcastProfileReducer) AppendEvents(events ...eventstore.Event) {
	r.pending = append(r.pending, events...)
}

func (r *upcastProfileReducer) Reduce() error {
	for _, event := range r.pending {
		profile := event.(*upcastProfileEvent)
		r.names = append(r.names, profile.DisplayName)
		r.actives = append(r.actives, profile.Active)
	}
	r.pending = r.pending[:0]
	return nil
}

// mappedEvent is used by the unconfigured-event compatibility test.
type mappedEvent struct {
	eventstore.BaseEvent `json:"-"`
	Description          string
}

func newUpcastTestEventstore(t *testing.T, registry *eventstore.EventUpcasterRegistry, envelopes ...eventstore.Event) *eventstore.Eventstore {
	t.Helper()
	eventstore.RegisterFilterEventMapper(upcastAggregateType, upcastEventType, upcastProfileMapper)
	return eventstore.NewEventstore(&eventstore.Config{
		Querier:   &upcastFakeQuerier{events: envelopes},
		Upcasters: registry,
	})
}

func upcastQuery() *eventstore.SearchQueryBuilder {
	return eventstore.NewSearchQueryBuilder(eventstore.ColumnsEvent).
		AddQuery().
		AggregateTypes(upcastAggregateType).
		Builder()
}

// TestUpcaster_ThreeGenerations verifies that v1 and v2 payloads are
// transformed along the fixed chain to the current v3 protocol and that the
// mapper/reducer produce the same projection as native v3 events.
func TestUpcaster_ThreeGenerations(t *testing.T) {
	v1Bytes, err := json.Marshal(upcastPayloadV1{Name: "hodor"})
	if err != nil {
		t.Fatal(err)
	}
	v2Bytes, err := json.Marshal(upcastPayloadV2{DisplayName: "hodor"})
	if err != nil {
		t.Fatal(err)
	}
	v3Bytes, err := json.Marshal(upcastPayloadV3{DisplayName: "hodor", Active: true})
	if err != nil {
		t.Fatal(err)
	}

	registry := eventstore.NewEventUpcasterRegistry()
	if err := registry.Register(upcastAggregateType, upcastEventType, 1, 2, upcastV1ToV2); err != nil {
		t.Fatalf("register v1->v2: %v", err)
	}
	if err := registry.Register(upcastAggregateType, upcastEventType, 2, 3, upcastV2ToV3); err != nil {
		t.Fatalf("register v2->v3: %v", err)
	}

	historical := []eventstore.Event{
		newUpcastEnvelope(upcastAggregateType, upcastEventType, 1, 1, v1Bytes),
		newUpcastEnvelope(upcastAggregateType, upcastEventType, 2, 2, v2Bytes),
	}
	current := []eventstore.Event{
		newUpcastEnvelope(upcastAggregateType, upcastEventType, 3, 1, v3Bytes),
		newUpcastEnvelope(upcastAggregateType, upcastEventType, 3, 2, v3Bytes),
	}

	// Keep references to the raw historical bytes and sort keys; the
	// eventstore history must never be rewritten.
	rawV1 := append([]byte(nil), historical[0].DataAsBytes()...)
	rawV2 := append([]byte(nil), historical[1].DataAsBytes()...)
	wantKeys := []eventstore.EventSortKey{
		eventstore.EventSortKeyFromEvent(historical[0]),
		eventstore.EventSortKeyFromEvent(historical[1]),
	}

	es := newUpcastTestEventstore(t, registry, historical...)
	ctx := authz.NewMockContext("instance-1", "org-1", "user-1")

	historicalReducer := new(upcastProfileReducer)
	if err := es.FilterToReducer(ctx, upcastQuery(), historicalReducer); err != nil {
		t.Fatalf("filter historical events: %v", err)
	}

	currentES := newUpcastTestEventstore(t, eventstore.NewEventUpcasterRegistry(), current...)
	currentReducer := new(upcastProfileReducer)
	if err := currentES.FilterToReducer(ctx, upcastQuery(), currentReducer); err != nil {
		t.Fatalf("filter current events: %v", err)
	}

	if !reflect.DeepEqual(historicalReducer.names, currentReducer.names) {
		t.Errorf("projected names differ: historical=%v current=%v", historicalReducer.names, currentReducer.names)
	}
	if !reflect.DeepEqual(historicalReducer.actives, currentReducer.actives) {
		t.Errorf("projected actives differ: historical=%v current=%v", historicalReducer.actives, currentReducer.actives)
	}
	if !reflect.DeepEqual(historicalReducer.names, []string{"hodor", "hodor"}) {
		t.Errorf("unexpected projected names: %v", historicalReducer.names)
	}
	if !reflect.DeepEqual(historicalReducer.actives, []bool{true, true}) {
		t.Errorf("unexpected projected actives: %v", historicalReducer.actives)
	}

	// The stored envelopes keep their original payload bytes and revision.
	if !reflect.DeepEqual(historical[0].DataAsBytes(), rawV1) {
		t.Errorf("historical v1 bytes were modified: got %s want %s", historical[0].DataAsBytes(), rawV1)
	}
	if !reflect.DeepEqual(historical[1].DataAsBytes(), rawV2) {
		t.Errorf("historical v2 bytes were modified: got %s want %s", historical[1].DataAsBytes(), rawV2)
	}
	if historical[0].Revision() != 1 || historical[1].Revision() != 2 {
		t.Errorf("historical revisions were modified: %d %d", historical[0].Revision(), historical[1].Revision())
	}

	// The mapped events carry the full, unchanged EventSortKey and all
	// protected envelope fields, while presenting the target revision.
	mapped, err := es.Filter(ctx, upcastQuery())
	if err != nil {
		t.Fatalf("filter mapped events: %v", err)
	}
	if len(mapped) != 2 {
		t.Fatalf("expected 2 mapped events, got %d", len(mapped))
	}
	for i, event := range mapped {
		gotKey := eventstore.EventSortKeyFromEvent(event)
		if !reflect.DeepEqual(gotKey, wantKeys[i]) {
			t.Errorf("event %d sort key changed: got %+v want %+v", i, gotKey, wantKeys[i])
		}
		agg := event.Aggregate()
		if agg.InstanceID != "instance-1" || agg.ID != "aggregate-1" || agg.ResourceOwner != "org-1" {
			t.Errorf("identity fields changed: %+v", agg)
		}
		if event.Sequence() != wantKeys[i].Sequence ||
			!event.Position().Equal(wantKeys[i].Position) ||
			event.InTxOrder() != wantKeys[i].InTxOrder {
			t.Errorf("ordering fields changed on event %d", i)
		}
		if event.Revision() != 3 || agg.Version != "v3" {
			t.Errorf("event %d not presented at target revision: revision=%d version=%s", i, event.Revision(), agg.Version)
		}
		if event.Creator() != "user-1" {
			t.Errorf("creator changed: %s", event.Creator())
		}
	}
}

// TestUpcaster_PayloadCopyAndReadOnlyMeta ensures transformers only receive
// private payload copies and read-only metadata: mutating the input can
// never affect the envelope, and transformers have no way to return new
// metadata.
func TestUpcaster_PayloadCopyAndReadOnlyMeta(t *testing.T) {
	v1Bytes, _ := json.Marshal(upcastPayloadV1{Name: "hodor"})
	envelope := newUpcastEnvelope(upcastAggregateType, upcastEventType, 1, 7, v1Bytes)
	raw := append([]byte(nil), v1Bytes...)

	var seenMeta eventstore.UpcastMeta
	registry := eventstore.NewEventUpcasterRegistry()
	err := registry.Register(upcastAggregateType, upcastEventType, 1, 2, func(in *eventstore.UpcastInput) ([]byte, error) {
		seenMeta = in.Meta
		// Try to tamper with the copy and the metadata copy.
		in.Payload[0] = 'X'
		in.Meta.Sequence = 999
		in.Meta.AggregateID = "hacked"
		return json.Marshal(upcastPayloadV2{DisplayName: "hodor"})
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	mapped, err := registry.Snapshot().Upcast(envelope)
	if err != nil {
		t.Fatalf("upcast: %v", err)
	}

	if !reflect.DeepEqual(envelope.DataAsBytes(), raw) {
		t.Errorf("envelope payload was mutated through the transformer input: %s", envelope.DataAsBytes())
	}
	if envelope.Sequence() != 7 || envelope.Aggregate().ID != "aggregate-1" {
		t.Errorf("envelope metadata was mutated through the read-only meta copy")
	}
	if seenMeta.Sequence != 7 || seenMeta.AggregateID != "aggregate-1" {
		t.Errorf("transformer did not receive the expected read-only metadata: %+v", seenMeta)
	}
	if mapped.Sequence() != 7 || mapped.Aggregate().ID != "aggregate-1" {
		t.Errorf("upcast event changed protected metadata: seq=%d id=%s", mapped.Sequence(), mapped.Aggregate().ID)
	}
	if mapped.Aggregate().ResourceOwner != "org-1" || mapped.Aggregate().InstanceID != "instance-1" ||
		mapped.InTxOrder() != 7 || !mapped.Position().Equal(decimal.NewFromInt(1007)) {
		t.Errorf("upcast event changed identity or ordering fields: %+v", mapped.Aggregate())
	}
}

// TestUpcaster_RegistrationValidation covers duplicate edges, invalid
// input, cycles and missing intermediate revisions.
func TestUpcaster_RegistrationValidation(t *testing.T) {
	noop := func(*eventstore.UpcastInput) ([]byte, error) { return []byte("{}"), nil }

	t.Run("invalid input", func(t *testing.T) {
		registry := eventstore.NewEventUpcasterRegistry()
		cases := []struct {
			name        string
			aggregate   eventstore.AggregateType
			event       eventstore.EventType
			from, to    uint16
			transformer eventstore.PayloadTransformer
		}{
			{"empty aggregate", "", "event", 1, 2, noop},
			{"empty event", "aggregate", "", 1, 2, noop},
			{"nil transformer", "aggregate", "event", 1, 2, nil},
			{"zero from", "aggregate", "event", 0, 2, noop},
			{"zero to", "aggregate", "event", 1, 0, noop},
			{"same revision", "aggregate", "event", 2, 2, noop},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				err := registry.Register(tc.aggregate, tc.event, tc.from, tc.to, tc.transformer)
				if !errors.Is(err, eventstore.ErrInvalidUpcast) {
					t.Fatalf("want ErrInvalidUpcast, got %v", err)
				}
			})
		}
	})

	t.Run("duplicate edge", func(t *testing.T) {
		registry := eventstore.NewEventUpcasterRegistry()
		if err := registry.Register("aggregate", "event", 1, 2, noop); err != nil {
			t.Fatal(err)
		}
		err := registry.Register("aggregate", "event", 1, 2, noop)
		if !errors.Is(err, eventstore.ErrDuplicateUpcastEdge) {
			t.Fatalf("want ErrDuplicateUpcastEdge, got %v", err)
		}
	})

	t.Run("cycle", func(t *testing.T) {
		registry := eventstore.NewEventUpcasterRegistry()
		if err := registry.Register("aggregate", "event", 1, 2, noop); err != nil {
			t.Fatal(err)
		}
		err := registry.Register("aggregate", "event", 2, 1, noop)
		if !errors.Is(err, eventstore.ErrCyclicUpcast) {
			t.Fatalf("want ErrCyclicUpcast, got %v", err)
		}
		// The failed registration leaves the registry unchanged.
		if registry.Version() != 1 {
			t.Errorf("generation version = %d, want 1", registry.Version())
		}
	})

	t.Run("missing intermediate revision", func(t *testing.T) {
		registry := eventstore.NewEventUpcasterRegistry()
		if err := registry.Register("aggregate", "event", 1, 2, noop); err != nil {
			t.Fatal(err)
		}
		// Target becomes 4, but revision 2 has no path to 4.
		err := registry.Register("aggregate", "event", 3, 4, noop)
		if !errors.Is(err, eventstore.ErrMissingIntermediateRevision) {
			t.Fatalf("want ErrMissingIntermediateRevision, got %v", err)
		}
	})

	t.Run("failed registration does not publish generation", func(t *testing.T) {
		registry := eventstore.NewEventUpcasterRegistry()
		before := registry.Snapshot()
		if err := registry.Register("aggregate", "event", 1, 2, noop); err != nil {
			t.Fatal(err)
		}
		if err := registry.Register("aggregate", "event", 2, 1, noop); err == nil {
			t.Fatal("expected cycle error")
		}
		after := registry.Snapshot()
		if before.Version() != 0 || after.Version() != 1 {
			t.Errorf("unexpected versions: before=%d after=%d", before.Version(), after.Version())
		}
	})
}

// TestUpcaster_Forks verifies that ambiguous forks are rejected while
// prioritized or equivalent-target forks resolve deterministically.
func TestUpcaster_Forks(t *testing.T) {
	transform := func(marker string) eventstore.PayloadTransformer {
		return func(in *eventstore.UpcastInput) ([]byte, error) {
			return json.Marshal(map[string]any{"from": in.Meta.FromRevision, "marker": marker})
		}
	}

	t.Run("ambiguous fork is rejected", func(t *testing.T) {
		registry := eventstore.NewEventUpcasterRegistry()
		for _, edge := range []struct{ from, to uint16 }{
			{1, 2}, {2, 4}, {1, 3},
		} {
			if err := registry.Register("aggregate", "fork", edge.from, edge.to, transform("x")); err != nil {
				t.Fatal(err)
			}
		}
		err := registry.Register("aggregate", "fork", 3, 4, transform("x"))
		if !errors.Is(err, eventstore.ErrAmbiguousUpcast) {
			t.Fatalf("want ErrAmbiguousUpcast, got %v", err)
		}
	})

	t.Run("priority resolves the fork deterministically", func(t *testing.T) {
		registry := eventstore.NewEventUpcasterRegistry()
		mustRegister := func(from, to uint16, marker string, opts ...eventstore.UpcastOption) {
			t.Helper()
			if err := registry.Register("aggregate", "fork-prio", from, to, transform(marker), opts...); err != nil {
				t.Fatalf("register %d->%d: %v", from, to, err)
			}
		}
		// The branch through revision 2 is explicitly preferred.
		mustRegister(1, 2, "via-v2", eventstore.WithUpcastPriority(1))
		mustRegister(2, 4, "via-v2")
		mustRegister(1, 3, "via-v3")
		mustRegister(3, 4, "via-v3")

		envelope := newUpcastEnvelope("aggregate", "fork-prio", 1, 1, []byte(`{}`))
		for i := 0; i < 50; i++ {
			mapped, err := registry.Snapshot().Upcast(envelope)
			if err != nil {
				t.Fatalf("upcast iteration %d: %v", i, err)
			}
			var payload struct {
				Marker string `json:"marker"`
			}
			if err := json.Unmarshal(mapped.DataAsBytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Marker != "via-v2" {
				t.Fatalf("iteration %d took non-deterministic fork: %s", i, payload.Marker)
			}
			if mapped.Revision() != 4 {
				t.Fatalf("target revision = %d, want 4", mapped.Revision())
			}
		}
	})

	t.Run("equivalent target declaration resolves the fork deterministically", func(t *testing.T) {
		registry := eventstore.NewEventUpcasterRegistry()
		mustRegister := func(from, to uint16, marker string, opts ...eventstore.UpcastOption) {
			t.Helper()
			if err := registry.Register("aggregate", "fork-equiv", from, to, transform(marker), opts...); err != nil {
				t.Fatalf("register %d->%d: %v", from, to, err)
			}
		}
		mustRegister(1, 2, "via-v2", eventstore.WithEquivalentPaths())
		mustRegister(2, 4, "via-v2")
		mustRegister(1, 3, "via-v3")
		mustRegister(3, 4, "via-v3")

		envelope := newUpcastEnvelope("aggregate", "fork-equiv", 1, 1, []byte(`{}`))
		// Deterministic tiebreak: the edge to the lower revision (2) wins.
		mapped, err := registry.Snapshot().Upcast(envelope)
		if err != nil {
			t.Fatal(err)
		}
		var payload struct {
			Marker string `json:"marker"`
		}
		if err := json.Unmarshal(mapped.DataAsBytes(), &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Marker != "via-v2" {
			t.Fatalf("equivalent fork was not resolved deterministically: %s", payload.Marker)
		}
	})
}

// TestUpcaster_UnsupportedRevision ensures unknown or unreachable revisions
// fail explicitly instead of being deserialized into zero values.
func TestUpcaster_UnsupportedRevision(t *testing.T) {
	registry := eventstore.NewEventUpcasterRegistry()
	if err := registry.Register(upcastAggregateType, upcastEventType, 2, 3, upcastV2ToV3); err != nil {
		t.Fatal(err)
	}

	mapperCalled := false
	eventstore.RegisterFilterEventMapper(upcastAggregateType, upcastEventType, func(eventstore.Event) (eventstore.Event, error) {
		mapperCalled = true
		return &mappedEvent{}, nil
	})

	cases := []struct {
		name     string
		revision uint16
	}{
		{"older revision without path", 1},
		{"unknown newer revision", 99},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mapperCalled = false
			envelope := newUpcastEnvelope(upcastAggregateType, upcastEventType, tc.revision, 1, []byte(`{"name":"hodor"}`))
			_, err := registry.Snapshot().Upcast(envelope)
			if !errors.Is(err, eventstore.ErrUnsupportedRevision) {
				t.Fatalf("want ErrUnsupportedRevision, got %v", err)
			}

			es := eventstore.NewEventstore(&eventstore.Config{
				Querier:   &upcastFakeQuerier{events: []eventstore.Event{envelope}},
				Upcasters: registry,
			})
			_, err = es.Filter(authz.NewMockContext("instance-1", "org-1", "user-1"), upcastQuery())
			if !errors.Is(err, eventstore.ErrUnsupportedRevision) {
				t.Fatalf("Filter want ErrUnsupportedRevision, got %v", err)
			}
			if mapperCalled {
				t.Fatal("mapper must not run for an unsupported revision: unknown payloads must not be zero-value deserialized")
			}
		})
	}
}

// TestUpcaster_TransformerFailuresAndLimits covers transformer errors,
// error truncation, payload byte limits, step limits and invalid JSON.
func TestUpcaster_TransformerFailuresAndLimits(t *testing.T) {
	t.Run("transformer error is wrapped and truncated", func(t *testing.T) {
		registry := eventstore.NewEventUpcasterRegistry()
		longErr := errors.New(stringsRepeat("x", 2048))
		if err := registry.Register("aggregate", "event", 1, 2, func(*eventstore.UpcastInput) ([]byte, error) {
			return nil, longErr
		}); err != nil {
			t.Fatal(err)
		}
		_, err := registry.Snapshot().Upcast(newUpcastEnvelope("aggregate", "event", 1, 1, []byte(`{}`)))
		if !errors.Is(err, eventstore.ErrUpcastFailed) {
			t.Fatalf("want ErrUpcastFailed, got %v", err)
		}
		// The transformer's 2 KiB error must be truncated to the configured
		// error budget; the full original message must not be propagated.
		fullMessage := stringsRepeat("x", 2048)
		if strings.Contains(err.Error(), fullMessage) {
			t.Errorf("transformer error was not truncated: full message propagated")
		}
		if !strings.Contains(err.Error(), "...(truncated)") {
			t.Errorf("expected truncation marker in error: %s", err.Error())
		}
	})

	t.Run("payload byte limit on input", func(t *testing.T) {
		registry := eventstore.NewEventUpcasterRegistry(eventstore.WithUpcastLimits(eventstore.UpcastLimits{MaxPayloadBytes: 4, MaxSteps: 8, MaxErrorBytes: 64}))
		if err := registry.Register("aggregate", "event", 1, 2, func(in *eventstore.UpcastInput) ([]byte, error) {
			return in.Payload, nil
		}); err != nil {
			t.Fatal(err)
		}
		_, err := registry.Snapshot().Upcast(newUpcastEnvelope("aggregate", "event", 1, 1, []byte(`{"a":1}`)))
		if !errors.Is(err, eventstore.ErrPayloadTooLarge) {
			t.Fatalf("want ErrPayloadTooLarge, got %v", err)
		}
	})

	t.Run("payload byte limit on output", func(t *testing.T) {
		registry := eventstore.NewEventUpcasterRegistry(eventstore.WithUpcastLimits(eventstore.UpcastLimits{MaxPayloadBytes: 4, MaxSteps: 8, MaxErrorBytes: 64}))
		if err := registry.Register("aggregate", "event", 1, 2, func(*eventstore.UpcastInput) ([]byte, error) {
			return []byte(`{"a":1234}`), nil
		}); err != nil {
			t.Fatal(err)
		}
		_, err := registry.Snapshot().Upcast(newUpcastEnvelope("aggregate", "event", 1, 1, []byte(`{}`)))
		if !errors.Is(err, eventstore.ErrPayloadTooLarge) {
			t.Fatalf("want ErrPayloadTooLarge, got %v", err)
		}
	})

	t.Run("step limit", func(t *testing.T) {
		registry := eventstore.NewEventUpcasterRegistry(eventstore.WithUpcastLimits(eventstore.UpcastLimits{MaxPayloadBytes: 4096, MaxSteps: 2, MaxErrorBytes: 64}))
		noop := func(in *eventstore.UpcastInput) ([]byte, error) { return in.Payload, nil }
		for _, edge := range []struct{ from, to uint16 }{{1, 2}, {2, 3}, {3, 4}, {4, 5}} {
			if err := registry.Register("aggregate", "event", edge.from, edge.to, noop); err != nil {
				t.Fatal(err)
			}
		}
		_, err := registry.Snapshot().Upcast(newUpcastEnvelope("aggregate", "event", 1, 1, []byte(`{}`)))
		if !errors.Is(err, eventstore.ErrTooManyUpcastSteps) {
			t.Fatalf("want ErrTooManyUpcastSteps, got %v", err)
		}
	})

	t.Run("invalid json output", func(t *testing.T) {
		registry := eventstore.NewEventUpcasterRegistry()
		if err := registry.Register("aggregate", "event", 1, 2, func(*eventstore.UpcastInput) ([]byte, error) {
			return []byte(`{not json`), nil
		}); err != nil {
			t.Fatal(err)
		}
		_, err := registry.Snapshot().Upcast(newUpcastEnvelope("aggregate", "event", 1, 1, []byte(`{}`)))
		if !errors.Is(err, eventstore.ErrUpcastFailed) {
			t.Fatalf("want ErrUpcastFailed, got %v", err)
		}
	})
}

// stringsRepeat builds a long string without importing strings.
func stringsRepeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}

// TestUpcaster_GenerationSwitch verifies immutable generations: a pinned
// snapshot keeps using its own paths while hot registrations publish new
// generations, cache identity includes the registry version and one batch
// never mixes path sets.
func TestUpcaster_GenerationSwitch(t *testing.T) {
	registry := eventstore.NewEventUpcasterRegistry()
	if err := registry.Register(upcastAggregateType, upcastEventType, 1, 2, upcastV1ToV2); err != nil {
		t.Fatal(err)
	}

	genA := registry.Snapshot()
	if genA.Version() != 1 {
		t.Fatalf("genA version = %d, want 1", genA.Version())
	}

	v1Envelope := newUpcastEnvelope(upcastAggregateType, upcastEventType, 1, 1, []byte(`{"name":"hodor"}`))
	mappedA, err := genA.Upcast(v1Envelope)
	if err != nil {
		t.Fatal(err)
	}
	if mappedA.Revision() != 2 {
		t.Fatalf("genA target = %d, want 2", mappedA.Revision())
	}

	// Hot registration publishes generation 2 without touching genA.
	if err := registry.Register(upcastAggregateType, upcastEventType, 2, 3, upcastV2ToV3); err != nil {
		t.Fatal(err)
	}
	genB := registry.Snapshot()
	if genB.Version() != 2 {
		t.Fatalf("genB version = %d, want 2", genB.Version())
	}

	keyA := genA.CacheKey(upcastAggregateType, upcastEventType, 1)
	keyB := genB.CacheKey(upcastAggregateType, upcastEventType, 1)
	if keyA.Generation == keyB.Generation {
		t.Errorf("cache identity must include the registry version: %+v %+v", keyA, keyB)
	}
	if keyA.Revision != 1 || keyA.EventType != upcastEventType || keyA.AggregateType != upcastAggregateType {
		t.Errorf("cache identity misses type/revision fields: %+v", keyA)
	}

	// The old snapshot still resolves the old, shorter path.
	mappedA2, err := genA.Upcast(newUpcastEnvelope(upcastAggregateType, upcastEventType, 1, 1, []byte(`{"name":"hodor"}`)))
	if err != nil {
		t.Fatal(err)
	}
	if mappedA2.Revision() != 2 {
		t.Errorf("pinned generation changed underneath the Filter: revision=%d", mappedA2.Revision())
	}

	mappedB, err := genB.Upcast(newUpcastEnvelope(upcastAggregateType, upcastEventType, 1, 1, []byte(`{"name":"hodor"}`)))
	if err != nil {
		t.Fatal(err)
	}
	if mappedB.Revision() != 3 {
		t.Errorf("new generation target = %d, want 3", mappedB.Revision())
	}

	// Batch consistency through the Eventstore: every Filter must apply one
	// generation to all of its events even while registrations race with it.
	batch := []eventstore.Event{
		newUpcastEnvelope(upcastAggregateType, upcastEventType, 1, 1, []byte(`{"name":"hodor"}`)),
		newUpcastEnvelope(upcastAggregateType, upcastEventType, 2, 2, []byte(`{"displayName":"hodor"}`)),
	}
	es := newUpcastTestEventstore(t, registry, batch...)
	ctx := authz.NewMockContext("instance-1", "org-1", "user-1")

	var (
		filterWg    sync.WaitGroup
		registrarWg sync.WaitGroup
	)
	// Hot registrations on unrelated event types bump the registry version
	// concurrently with the filters, exercising the copy-on-write snapshots.
	stopRegistrations := make(chan struct{})
	registrarWg.Add(1)
	go func() {
		defer registrarWg.Done()
		for i := 0; ; i++ {
			select {
			case <-stopRegistrations:
				return
			default:
			}
			err := registry.Register("aggregate", eventstore.EventType(fmt.Sprintf("upcast.concurrent.%d", i)),
				1, 2, func(*eventstore.UpcastInput) ([]byte, error) { return []byte(`{}`), nil })
			if err != nil {
				t.Errorf("concurrent registration: %v", err)
				return
			}
		}
	}()

	for i := 0; i < 20; i++ {
		filterWg.Add(1)
		go func() {
			defer filterWg.Done()
			for j := 0; j < 25; j++ {
				events, err := es.Filter(ctx, upcastQuery())
				if err != nil {
					t.Errorf("concurrent filter: %v", err)
					return
				}
				revision := events[0].Revision()
				for _, event := range events[1:] {
					if event.Revision() != revision {
						t.Errorf("batch mixed registry generations: %v", revisionsOf(events))
						return
					}
				}
			}
		}()
	}
	filterWg.Wait()
	close(stopRegistrations)
	registrarWg.Wait()
}

func revisionsOf(events []eventstore.Event) []uint16 {
	revisions := make([]uint16, len(events))
	for i, event := range events {
		revisions[i] = event.Revision()
	}
	return revisions
}

// TestUpcaster_UnconfiguredEventsKeepOriginalPath verifies the
// RegisterFilterEventMapper compatibility promise: event types without an
// upcast configuration behave exactly as before, and a registered mapper
// still receives them.
func TestUpcaster_UnconfiguredEventsKeepOriginalPath(t *testing.T) {
	const eventType eventstore.EventType = "upcast.unconfigured.event"
	envelope := newUpcastEnvelope("aggregate", eventType, 1, 3, []byte(`{"description":"hodor"}`))
	raw := append([]byte(nil), envelope.DataAsBytes()...)

	eventstore.RegisterFilterEventMapper("aggregate", eventType, func(event eventstore.Event) (eventstore.Event, error) {
		return &mappedEvent{Description: "hodor", BaseEvent: *eventstore.BaseEventFromRepo(event)}, nil
	})

	registry := eventstore.NewEventUpcasterRegistry()
	es := eventstore.NewEventstore(&eventstore.Config{
		Querier:   &upcastFakeQuerier{events: []eventstore.Event{envelope}},
		Upcasters: registry,
	})

	events, err := es.Filter(authz.NewMockContext("instance-1", "org-1", "user-1"),
		eventstore.NewSearchQueryBuilder(eventstore.ColumnsEvent).AddQuery().AggregateTypes("aggregate").Builder())
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	mapped, ok := events[0].(*mappedEvent)
	if !ok {
		t.Fatalf("expected *mappedEvent, got %T", events[0])
	}
	if mapped.Description != "hodor" {
		t.Errorf("mapper did not run on the original path: %+v", mapped)
	}
	if !reflect.DeepEqual(envelope.DataAsBytes(), raw) {
		t.Errorf("unconfigured event bytes changed: %s", envelope.DataAsBytes())
	}
}

// TestUpcaster_TargetRevisionPassThrough ensures an event already written at
// the current revision bypasses the transformers entirely.
func TestUpcaster_TargetRevisionPassThrough(t *testing.T) {
	registry := eventstore.NewEventUpcasterRegistry()
	transformerCalled := false
	if err := registry.Register(upcastAggregateType, upcastEventType, 1, 2, func(*eventstore.UpcastInput) ([]byte, error) {
		transformerCalled = true
		return []byte(`{}`), nil
	}); err != nil {
		t.Fatal(err)
	}

	envelope := newUpcastEnvelope(upcastAggregateType, upcastEventType, 2, 1, []byte(`{"displayName":"hodor"}`))
	mapped, err := registry.Snapshot().Upcast(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if transformerCalled {
		t.Fatal("transformer must not run for an event already at the target revision")
	}
	if mapped != eventstore.Event(envelope) {
		t.Errorf("target-revision event must pass through unchanged")
	}
}

// TestUpcaster_DefaultRegistry exercises the package-level registration
// helpers. A unique event type per invocation keeps the global default
// registry clean across repeated test runs.
func TestUpcaster_DefaultRegistry(t *testing.T) {
	defaultRegistryEventCounter.Add(1)
	eventType := eventstore.EventType(fmt.Sprintf("upcast.default.registry.event.%d", defaultRegistryEventCounter.Load()))
	err := eventstore.RegisterEventUpcaster("aggregate", eventType, 1, 2, func(in *eventstore.UpcastInput) ([]byte, error) {
		return json.Marshal(map[string]string{"displayName": "hodor"})
	})
	if err != nil {
		t.Fatalf("RegisterEventUpcaster: %v", err)
	}
	envelope := newUpcastEnvelope("aggregate", eventType, 1, 1, []byte(`{"name":"hodor"}`))
	mapped, err := eventstore.DefaultUpcasterRegistry().Snapshot().Upcast(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if mapped.Revision() != 2 {
		t.Fatalf("default registry upcast revision = %d, want 2", mapped.Revision())
	}
}

var defaultRegistryEventCounter atomic.Int64
