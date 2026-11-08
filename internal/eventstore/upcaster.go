package eventstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"github.com/zitadel/zitadel/internal/zerrors"
)

// Sentinel errors describing upcaster registration and runtime failures.
// Runtime failures returned through the eventstore mapping path wrap these
// errors inside a [zerrors.ZitadelError], so errors.Is keeps working while
// the transport layers still get a typed error code.
var (
	// ErrInvalidUpcast is returned when an upcast registration is malformed.
	ErrInvalidUpcast = errors.New("eventstore: invalid upcast registration")
	// ErrDuplicateUpcastEdge is returned when two edges with the same
	// (aggregate type, event type, from, to) tuple are registered.
	ErrDuplicateUpcastEdge = errors.New("eventstore: duplicate upcast edge")
	// ErrCyclicUpcast is returned when registered edges form a cycle.
	ErrCyclicUpcast = errors.New("eventstore: cyclic upcast graph")
	// ErrAmbiguousUpcast is returned when more than one path can reach the
	// current revision without a declared priority or equivalent target.
	ErrAmbiguousUpcast = errors.New("eventstore: ambiguous upcast path")
	// ErrMissingIntermediateRevision is returned when a registered source
	// revision cannot reach the current revision because an intermediate
	// revision has no outgoing edge.
	ErrMissingIntermediateRevision = errors.New("eventstore: missing intermediate revision")
	// ErrUnsupportedRevision is returned when an event carries a revision
	// for which no transformation path to the current revision is known.
	ErrUnsupportedRevision = errors.New("eventstore: unsupported event payload revision")
	// ErrUpcastFailed is returned when a payload transformer fails.
	ErrUpcastFailed = errors.New("eventstore: payload upcast failed")
	// ErrPayloadTooLarge is returned when a payload exceeds the configured
	// byte limit before or after a transformation step.
	ErrPayloadTooLarge = errors.New("eventstore: upcast payload exceeds byte limit")
	// ErrTooManyUpcastSteps is returned when a transformation chain exceeds
	// the configured maximum number of steps.
	ErrTooManyUpcastSteps = errors.New("eventstore: upcast chain exceeds step limit")
)

const (
	// defaultMaxUpcastPayloadBytes limits the size of a payload that enters
	// or leaves a transformer. It guards against unbounded memory growth.
	defaultMaxUpcastPayloadBytes = 4 << 20
	// defaultMaxUpcastSteps bounds the number of edges a single event is
	// transformed along. Registration already rejects cycles, so a chain
	// longer than this indicates a misconfiguration.
	defaultMaxUpcastSteps = 64
	// defaultMaxUpcastErrorBytes caps the size of a transformer error
	// message that is attached to an upcast failure.
	defaultMaxUpcastErrorBytes = 1024
	// maxResolvedPaths bounds the number of paths explored while resolving
	// forks so path resolution stays bounded for pathological graphs.
	maxResolvedPaths = 32
)

// UpcastLimits contains the bounded-execution limits of an
// [EventUpcasterRegistry]. Zero values are replaced by the defaults.
type UpcastLimits struct {
	// MaxPayloadBytes is the maximum accepted JSON payload size before the
	// first transformation and after every intermediate step.
	MaxPayloadBytes int
	// MaxSteps is the maximum number of transformation edges a single event
	// may traverse.
	MaxSteps int
	// MaxErrorBytes is the maximum length of a transformer error message
	// that is propagated. Longer messages are truncated.
	MaxErrorBytes int
}

func (l UpcastLimits) withDefaults() UpcastLimits {
	if l.MaxPayloadBytes <= 0 {
		l.MaxPayloadBytes = defaultMaxUpcastPayloadBytes
	}
	if l.MaxSteps <= 0 {
		l.MaxSteps = defaultMaxUpcastSteps
	}
	if l.MaxErrorBytes <= 0 {
		l.MaxErrorBytes = defaultMaxUpcastErrorBytes
	}
	return l
}

// RegistryOption customizes a new [EventUpcasterRegistry].
type RegistryOption func(*EventUpcasterRegistry)

// WithUpcastLimits overrides the default bounded-execution limits.
func WithUpcastLimits(limits UpcastLimits) RegistryOption {
	return func(r *EventUpcasterRegistry) {
		r.limits = limits.withDefaults()
	}
}

// UpcastMeta is the limited, read-only metadata exposed to a
// [PayloadTransformer]. Every field is a value copy: transformers can read
// identity and ordering information, but they have no handle on the stored
// envelope and therefore cannot change the permission subject, sequence,
// position or any other ordering field.
type UpcastMeta struct {
	AggregateType AggregateType
	EventType     EventType
	FromRevision  uint16
	ToRevision    uint16

	InstanceID    string
	AggregateID   string
	ResourceOwner string
	Sequence      uint64
	Position      decimal.Decimal
	InTxOrder     uint32
	Creator       string
	CreatedAt     time.Time
}

// UpcastInput is passed to a [PayloadTransformer]. The Payload slice is a
// private copy of the event bytes; mutating it can never affect the stored
// event or another transformation.
type UpcastInput struct {
	// Payload is the JSON payload of the FromRevision, privately copied.
	Payload []byte
	// Meta is the limited, read-only metadata of the event.
	Meta UpcastMeta
}

// PayloadTransformer converts the JSON payload of one revision into the
// JSON payload of the next revision.
//
// Implementations must be pure: the same [UpcastInput] must always produce
// the same output, and the function must not depend on mutable state or
// perform I/O. They must also be bounded and non-blocking: the eventstore
// enforces payload byte and step limits, but Go cannot preempt or kill an
// arbitrarily blocking function, so liveness remains the implementer's
// responsibility. Only payload bytes are returned; a transformer has no way
// to rewrite envelope metadata, the permission subject or the sort key.
type PayloadTransformer func(input *UpcastInput) ([]byte, error)

type upcastOptions struct {
	priority    int
	hasPriority bool
	equivalent  bool
}

// UpcastOption customizes the registration of a transformation edge.
type UpcastOption func(*upcastOptions)

// WithUpcastPriority declares a deterministic priority for an edge at a fork.
// Lower values are preferred. When several paths can reach the current
// revision, the path built from the lowest-priority edges is selected.
// Forks without either an explicit priority or an equivalent-target
// declaration are rejected at registration.
func WithUpcastPriority(priority int) UpcastOption {
	return func(o *upcastOptions) {
		o.priority = priority
		o.hasPriority = true
	}
}

// WithEquivalentPaths declares that every branch of a fork for the
// (aggregate type, event type) pair converges to the same target protocol,
// i.e. the resulting payloads are semantically equivalent. Selection stays
// deterministic (the edge leading to the lowest next revision wins); the
// declaration only permits what would otherwise be an ambiguous graph.
func WithEquivalentPaths() UpcastOption {
	return func(o *upcastOptions) {
		o.equivalent = true
	}
}

type upcastTypeKey struct {
	aggregateType AggregateType
	eventType     EventType
}

func (k upcastTypeKey) String() string {
	return string(k.aggregateType) + "/" + string(k.eventType)
}

type upcastEdge struct {
	from        uint16
	to          uint16
	priority    int
	hasPriority bool
	transform   PayloadTransformer
}

// orderKey makes edge selection deterministic without ever iterating a map
// randomly: explicit-priority edges come first (lowest priority wins), all
// other edges are ordered by their target revision.
func (e upcastEdge) orderKey() [3]int {
	priorityFlag := 1
	if e.hasPriority {
		priorityFlag = 0
	}
	return [3]int{priorityFlag, e.priority, int(e.to)}
}

type upcastPath struct {
	edges []upcastEdge
}

// upcastGraph is an immutable, validated revision DAG for one
// (aggregate type, event type) pair. All path resolution happens at
// registration time; loading an event only walks the precomputed path.
type upcastGraph struct {
	key        upcastTypeKey
	target     uint16
	equivalent bool
	// adjacency holds the outgoing edges per revision in deterministic
	// order, never relying on map iteration.
	adjacency map[uint16][]upcastEdge
	// paths is the resolved, deterministic path from every registered
	// source revision to the target revision.
	paths map[uint16]upcastPath
}

// UpcastCacheKey is the identity of a resolved transformation path. It
// contains the event type, the source payload revision and the registry
// generation: a hot registration produces a new generation version, so
// cached paths can never be reused across generations.
type UpcastCacheKey struct {
	AggregateType AggregateType
	EventType     EventType
	Revision      uint16
	Generation    uint64
}

// UpcastGeneration is an immutable snapshot of an [EventUpcasterRegistry].
// A single Filter call pins one generation and therefore applies one
// consistent set of transformation paths to every event of the batch, even
// if registrations happen concurrently.
type UpcastGeneration interface {
	// Version returns the registry generation version of the snapshot.
	Version() uint64
	// Upcast transforms the event payload along the resolved path. The
	// stored event is never mutated and nothing is written back to the
	// eventstore. Unconfigured events and events at the current revision
	// are returned unchanged.
	Upcast(event Event) (Event, error)
	// CacheKey returns the cache identity of the path resolved for an event.
	CacheKey(aggregateType AggregateType, eventType EventType, revision uint16) UpcastCacheKey
}

// upcastGeneration is an immutable snapshot of the registry.
type upcastGeneration struct {
	version uint64
	limits  UpcastLimits
	graphs  map[upcastTypeKey]*upcastGraph
}

// Version implements [UpcastGeneration].
func (g *upcastGeneration) Version() uint64 {
	return g.version
}

// CacheKey implements [UpcastGeneration].
func (g *upcastGeneration) CacheKey(aggregateType AggregateType, eventType EventType, revision uint16) UpcastCacheKey {
	return UpcastCacheKey{
		AggregateType: aggregateType,
		EventType:     eventType,
		Revision:      revision,
		Generation:    g.version,
	}
}

// cacheKey returns the cache identity of the resolved path for an event.
func (g *upcastGeneration) cacheKey(key upcastTypeKey, revision uint16) UpcastCacheKey {
	return g.CacheKey(key.aggregateType, key.eventType, revision)
}

// path returns the precomputed transformation path for a source revision.
func (g *upcastGeneration) path(key upcastTypeKey, revision uint16) (upcastPath, bool) {
	graph, ok := g.graphs[key]
	if !ok {
		return upcastPath{}, false
	}
	path, ok := graph.paths[revision]
	return path, ok
}

// Upcast transforms the payload of a stored event along the resolved path
// of the pinned generation. The original event is never mutated and nothing
// is written back to the eventstore.
func (g *upcastGeneration) Upcast(event Event) (Event, error) {
	if event == nil {
		return event, nil
	}
	agg := event.Aggregate()
	key := upcastTypeKey{
		aggregateType: agg.Type,
		eventType:     event.Type(),
	}
	graph, ok := g.graphs[key]
	if !ok {
		// Unconfigured events keep using the original mapping path.
		return event, nil
	}

	revision := event.Revision()
	if revision == graph.target {
		// Already at the mapper revision: no transformation needed.
		return event, nil
	}

	path, ok := graph.paths[revision]
	if !ok {
		return nil, g.unsupportedError(key, revision, graph.target)
	}

	// Hand out a private copy of the payload so the transformer can never
	// alias the bytes of the stored envelope.
	data := cloneBytes(event.DataAsBytes())
	if len(data) > g.limits.MaxPayloadBytes {
		return nil, g.wrap(ErrPayloadTooLarge, "ES-Upc01",
			fmt.Sprintf("payload of %s/%d is %d bytes, limit is %d",
				key, revision, len(data), g.limits.MaxPayloadBytes))
	}

	meta := UpcastMeta{
		AggregateType: agg.Type,
		EventType:     event.Type(),
		FromRevision:  revision,
		InstanceID:    agg.InstanceID,
		AggregateID:   agg.ID,
		ResourceOwner: agg.ResourceOwner,
		Sequence:      event.Sequence(),
		Position:      event.Position(),
		InTxOrder:     event.InTxOrder(),
		Creator:       event.Creator(),
		CreatedAt:     event.CreatedAt(),
	}

	for step, edge := range path.edges {
		if step >= g.limits.MaxSteps {
			return nil, g.wrap(ErrTooManyUpcastSteps, "ES-Upc02",
				fmt.Sprintf("upcast of %s from revision %d exceeds %d steps",
					key, revision, g.limits.MaxSteps))
		}

		meta.FromRevision = edge.from
		meta.ToRevision = edge.to
		out, err := edge.transform(&UpcastInput{
			Payload: cloneBytes(data),
			Meta:    meta,
		})
		if err != nil {
			return nil, g.wrap(ErrUpcastFailed, "ES-Upc03",
				fmt.Sprintf("transform %s/%d->%d failed: %s",
					key, edge.from, edge.to, truncateError(err, g.limits.MaxErrorBytes)))
		}
		if len(out) > g.limits.MaxPayloadBytes {
			return nil, g.wrap(ErrPayloadTooLarge, "ES-Upc01",
				fmt.Sprintf("payload of %s/%d is %d bytes after step %d, limit is %d",
					key, edge.to, len(out), step+1, g.limits.MaxPayloadBytes))
		}
		if !json.Valid(out) {
			return nil, g.wrap(ErrUpcastFailed, "ES-Upc03",
				fmt.Sprintf("transform %s/%d->%d returned invalid json",
					key, edge.from, edge.to))
		}
		// Keep an owned copy of the produced bytes as well.
		data = cloneBytes(out)
	}

	return &upcastEvent{
		Event:   event,
		data:    data,
		version: Version("v" + strconv.FormatUint(uint64(graph.target), 10)),
	}, nil
}

func (g *upcastGeneration) unsupportedError(key upcastTypeKey, revision, target uint16) error {
	return g.wrap(ErrUnsupportedRevision, "ES-Upc04",
		fmt.Sprintf("no upcast path for %s from revision %d to current revision %d",
			key, revision, target))
}

// wrap turns a sentinel failure into a [zerrors.ZitadelError] while keeping
// the sentinel reachable through errors.Is.
func (g *upcastGeneration) wrap(sentinel error, id, message string) error {
	return zerrors.ThrowPreconditionFailed(fmt.Errorf("%w: %s", sentinel, message), id, message)
}

func truncateError(err error, maxBytes int) string {
	message := err.Error()
	if len(message) <= maxBytes {
		return message
	}
	return message[:maxBytes] + "...(truncated)"
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

// upcastEvent presents the transformed payload to the registered event
// mapper while delegating every envelope field to the stored event. The
// only overrides are the payload bytes and the aggregate version, so the
// mapper and reducers see an event that is indistinguishable from one
// originally written at the target revision, while InstanceID,
// AggregateID, ResourceOwner, Sequence, Position and InTxOrder stay intact.
type upcastEvent struct {
	Event
	data    []byte
	version Version
}

func (e *upcastEvent) DataAsBytes() []byte {
	return e.data
}

func (e *upcastEvent) Unmarshal(ptr any) error {
	if len(e.data) == 0 {
		return nil
	}
	return json.Unmarshal(e.data, ptr)
}

func (e *upcastEvent) Revision() uint16 {
	revision, err := strconv.ParseUint(strings.TrimPrefix(string(e.version), "v"), 10, 16)
	if err != nil {
		return 0
	}
	return uint16(revision)
}

func (e *upcastEvent) Aggregate() *Aggregate {
	agg := *e.Event.Aggregate()
	agg.Version = e.version
	return &agg
}

// EventUpcasterRegistry holds the explicit revision transformation graph.
// Registrations are validated eagerly and published as immutable
// generations; reads never block on registrations and always observe a
// complete, consistent graph.
type EventUpcasterRegistry struct {
	mu      sync.RWMutex
	version uint64
	limits  UpcastLimits
	graphs  map[upcastTypeKey]*upcastGraph
}

// NewEventUpcasterRegistry creates an empty registry.
func NewEventUpcasterRegistry(opts ...RegistryOption) *EventUpcasterRegistry {
	r := &EventUpcasterRegistry{
		limits: UpcastLimits{}.withDefaults(),
		graphs: make(map[upcastTypeKey]*upcastGraph),
	}
	for _, opt := range opts {
		opt(r)
	}
	r.limits = r.limits.withDefaults()
	return r
}

// Snapshot returns the current immutable generation. The returned snapshot
// is never mutated by later registrations, so a Filter call can safely pin
// it for the whole event batch.
func (r *EventUpcasterRegistry) Snapshot() UpcastGeneration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return &upcastGeneration{
		version: r.version,
		limits:  r.limits,
		graphs:  r.graphs,
	}
}

// Version returns the current registry generation version. It is bumped on
// every successful registration.
func (r *EventUpcasterRegistry) Version() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.version
}

// Register adds a pure transformation edge from revision from to revision
// to for one (aggregate type, event type) pair. The whole graph is
// revalidated on every registration: duplicate edges, cycles, unreachable
// intermediate revisions and unresolved forks are rejected and leave the
// registry unchanged.
func (r *EventUpcasterRegistry) Register(
	aggregateType AggregateType,
	eventType EventType,
	from, to uint16,
	transform PayloadTransformer,
	opts ...UpcastOption,
) error {
	if aggregateType == "" || eventType == "" || transform == nil || from == 0 || to == 0 || from == to {
		return fmt.Errorf("%w: aggregateType=%q eventType=%q from=%d to=%d",
			ErrInvalidUpcast, aggregateType, eventType, from, to)
	}

	options := upcastOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	next, err := r.buildNextGeneration(upcastTypeKey{aggregateType: aggregateType, eventType: eventType},
		upcastEdge{
			from:        from,
			to:          to,
			priority:    options.priority,
			hasPriority: options.hasPriority,
			transform:   transform,
		},
		options.equivalent)
	if err != nil {
		return err
	}

	r.graphs = next
	r.version++
	return nil
}

func (r *EventUpcasterRegistry) buildNextGeneration(key upcastTypeKey, edge upcastEdge, equivalent bool) (map[upcastTypeKey]*upcastGraph, error) {
	// Copy-on-write: clone every graph so the previously published
	// generation stays immutable.
	next := make(map[upcastTypeKey]*upcastGraph, len(r.graphs)+1)
	for k, graph := range r.graphs {
		next[k] = graph.clone()
	}

	graph, ok := next[key]
	if !ok {
		graph = &upcastGraph{
			key:       key,
			adjacency: make(map[uint16][]upcastEdge),
			paths:     make(map[uint16]upcastPath),
		}
		next[key] = graph
	}
	if equivalent {
		graph.equivalent = true
	}

	for _, existing := range graph.adjacency[edge.from] {
		if existing.to == edge.to {
			return nil, fmt.Errorf("%w: %s %d->%d", ErrDuplicateUpcastEdge, key, edge.from, edge.to)
		}
	}

	graph.adjacency[edge.from] = insertEdgeSorted(graph.adjacency[edge.from], edge)

	if err := graph.detectCycle(); err != nil {
		return nil, err
	}

	graph.target = graph.resolveTarget()
	if err := graph.resolvePaths(); err != nil {
		return nil, err
	}
	return next, nil
}

func insertEdgeSorted(edges []upcastEdge, edge upcastEdge) []upcastEdge {
	idx := sort.Search(len(edges), func(i int) bool {
		return lessEdgeOrder(edge, edges[i])
	})
	edges = append(edges, upcastEdge{})
	copy(edges[idx+1:], edges[idx:])
	edges[idx] = edge
	return edges
}

// lessEdgeOrder defines the total, deterministic order of edges at a fork.
func lessEdgeOrder(a, b upcastEdge) bool {
	ka, kb := a.orderKey(), b.orderKey()
	for i := range ka {
		if ka[i] != kb[i] {
			return ka[i] < kb[i]
		}
	}
	return false
}

func (g *upcastGraph) clone() *upcastGraph {
	clone := &upcastGraph{
		key:        g.key,
		target:     g.target,
		equivalent: g.equivalent,
		adjacency:  make(map[uint16][]upcastEdge, len(g.adjacency)),
		paths:      make(map[uint16]upcastPath, len(g.paths)),
	}
	for revision, edges := range g.adjacency {
		clone.adjacency[revision] = append([]upcastEdge(nil), edges...)
	}
	for revision, path := range g.paths {
		clone.paths[revision] = upcastPath{edges: append([]upcastEdge(nil), path.edges...)}
	}
	return clone
}

// resolveTarget defines the current mapper revision as the highest revision
// an edge produces. Events already carrying this revision are passed through.
func (g *upcastGraph) resolveTarget() uint16 {
	var target uint16
	for _, edges := range g.adjacency {
		for _, edge := range edges {
			if edge.to > target {
				target = edge.to
			}
		}
	}
	return target
}

func (g *upcastGraph) detectCycle() error {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[uint16]int)
	var visit func(revision uint16) error
	visit = func(revision uint16) error {
		switch color[revision] {
		case gray:
			return fmt.Errorf("%w: %s involves revision %d", ErrCyclicUpcast, g.key, revision)
		case black:
			return nil
		}
		color[revision] = gray
		for _, edge := range g.adjacency[revision] {
			if err := visit(edge.to); err != nil {
				return err
			}
		}
		color[revision] = black
		return nil
	}
	// Iterate source revisions in sorted order so error reporting is stable.
	sources := sortedRevisions(g.adjacency)
	for _, revision := range sources {
		if err := visit(revision); err != nil {
			return err
		}
	}
	return nil
}

// resolvePaths precomputes the deterministic path of every registered
// source revision to the target and validates reachability and forks.
func (g *upcastGraph) resolvePaths() error {
	g.paths = make(map[uint16]upcastPath, len(g.adjacency))

	for _, source := range sortedRevisions(g.adjacency) {
		paths := g.collectPaths(source, g.target)
		switch len(paths) {
		case 0:
			return fmt.Errorf("%w: %s revision %d cannot reach target revision %d",
				ErrMissingIntermediateRevision, g.key, source, g.target)
		case 1:
			g.paths[source] = upcastPath{edges: paths[0]}
		default:
			if err := g.checkForkResolution(source, paths); err != nil {
				return err
			}
			// Edges are stored in deterministic order and the DFS follows
			// that order, so the first path is the declared winner.
			g.paths[source] = upcastPath{edges: paths[0]}
		}
	}
	return nil
}

// checkForkResolution ensures multiple reachable paths are only accepted
// with an explicit priority on every fork or an equivalent-target
// declaration. Map iteration order is never part of the decision.
func (g *upcastGraph) checkForkResolution(source uint16, paths [][]upcastEdge) error {
	if g.equivalent {
		return nil
	}
	forks := make(map[uint16]bool)
	for _, path := range paths {
		for _, edge := range path {
			if len(g.adjacency[edge.from]) > 1 {
				forks[edge.from] = true
			}
		}
	}
	for revision := range forks {
		if !g.forkHasPriority(revision) {
			return fmt.Errorf("%w: %s revision %d reaches target %d through %d unresolved paths",
				ErrAmbiguousUpcast, g.key, source, g.target, len(paths))
		}
	}
	return nil
}

// forkHasPriority reports whether at least one outgoing edge of the fork
// carries an explicit priority, which deterministically resolves it.
func (g *upcastGraph) forkHasPriority(revision uint16) bool {
	for _, edge := range g.adjacency[revision] {
		if edge.hasPriority {
			return true
		}
	}
	return false
}

// collectPaths enumerates the simple paths from source to target in the
// deterministic edge order, capped by [maxResolvedPaths].
func (g *upcastGraph) collectPaths(source, target uint16) [][]upcastEdge {
	var (
		paths   [][]upcastEdge
		current []upcastEdge
		visited = map[uint16]bool{source: true}
	)
	var walk func(revision uint16)
	walk = func(revision uint16) {
		if len(paths) >= maxResolvedPaths {
			return
		}
		if revision == target {
			paths = append(paths, append([]upcastEdge(nil), current...))
			return
		}
		for _, edge := range g.adjacency[revision] {
			if visited[edge.to] {
				continue
			}
			visited[edge.to] = true
			current = append(current, edge)
			walk(edge.to)
			current = current[:len(current)-1]
			visited[edge.to] = false
		}
	}
	walk(source)
	return paths
}

func sortedRevisions[V any](m map[uint16]V) []uint16 {
	revisions := make([]uint16, 0, len(m))
	for revision := range m {
		revisions = append(revisions, revision)
	}
	sort.Slice(revisions, func(i, j int) bool { return revisions[i] < revisions[j] })
	return revisions
}

// defaultUpcasterRegistry backs the package-level registration helpers so
// upcasters can be registered from init() functions just like
// [RegisterFilterEventMapper].
var defaultUpcasterRegistry = NewEventUpcasterRegistry()

// DefaultUpcasterRegistry returns the package-level registry populated by
// [RegisterEventUpcaster] and used by Eventstores without an explicit
// [Config.Upcasters]. Prefer wiring an explicit registry through the config.
func DefaultUpcasterRegistry() *EventUpcasterRegistry {
	return defaultUpcasterRegistry
}

// RegisterEventUpcaster registers a transformation edge on the package-level
// default registry. Prefer wiring an explicit [EventUpcasterRegistry] through
// [Config]; this helper exists for init()-time registration.
func RegisterEventUpcaster(
	aggregateType AggregateType,
	eventType EventType,
	from, to uint16,
	transform PayloadTransformer,
	opts ...UpcastOption,
) error {
	return defaultUpcasterRegistry.Register(aggregateType, eventType, from, to, transform, opts...)
}

// MustRegisterEventUpcaster registers on the default registry and panics on
// an invalid graph. It is intended for init()-time registrations where a
// broken transformation graph is a programmer error.
func MustRegisterEventUpcaster(
	aggregateType AggregateType,
	eventType EventType,
	from, to uint16,
	transform PayloadTransformer,
	opts ...UpcastOption,
) {
	if err := RegisterEventUpcaster(aggregateType, eventType, from, to, transform, opts...); err != nil {
		panic(err)
	}
}
