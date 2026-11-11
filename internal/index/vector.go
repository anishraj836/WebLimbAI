package index

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/crawler-monorepo/common/stopwords"
)

// Subword N-Gram Vector Store

type VectorSearchResult struct {
	DocID      string  `json:"doc_id"`
	Similarity float64 `json:"similarity"`
}

func CosineSimilarity(u, v []float64) float64 {
	if len(u) == 0 || len(v) == 0 || len(u) != len(v) {
		return 0.0
	}

	var dotProduct float64
	var normU float64
	var normV float64

	for i := 0; i < len(u); i++ {
		dotProduct += u[i] * v[i]
		normU += u[i] * u[i]
		normV += v[i] * v[i]
	}

	if normU == 0 || normV == 0 {
		return 0.0
	}

	return dotProduct / (math.Sqrt(normU) * math.Sqrt(normV))
}

func hashFNV1a(s string, seed uint64) uint64 {
	h := seed
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

func GenerateFeatureVector(text string, dimensions int) []float64 {
	if dimensions <= 0 {
		dimensions = 128
	}

	vec := make([]float64, dimensions)
	rawWords := strings.Fields(strings.ToLower(text))
	if len(rawWords) == 0 {
		return vec
	}

	var features []string
	for _, raw := range rawWords {
		w := strings.Trim(raw, ".,!?:;\"'()[]{}<>-")
		if w == "" || stopwords.IsStopword(w) {
			continue
		}

		features = append(features, w)

		runes := []rune(w)
		n := len(runes)
		for i := 0; i <= n-3; i++ {
			features = append(features, string(runes[i:i+3]))
		}
		for i := 0; i <= n-4; i++ {
			features = append(features, string(runes[i:i+4]))
		}
	}

	if len(features) == 0 {
		return vec
	}

	for _, f := range features {
		h1 := hashFNV1a(f, 14695981039346656037)
		h2 := hashFNV1a(f, 0xcbf29ce484222325)

		idx := int(h1 % uint64(dimensions))
		sign := 1.0
		if (h2 & 1) != 0 {
			sign = -1.0
		}
		vec[idx] += sign
	}

	var norm float64
	for _, val := range vec {
		norm += val * val
	}

	if norm > 0 {
		mag := math.Sqrt(norm)
		for i := 0; i < len(vec); i++ {
			vec[i] /= mag
		}
	}

	return vec
}

// VectorIndex stores one normalized vector per document, spread across N
// shards by FNV-1a(docID). Adding or deleting a vector takes a read lock on
// the index (which only excludes structural changes such as SetPrecision or
// LoadSnapshot) plus the write lock of the one shard that owns the document,
// so concurrent indexing workers don't serialize on a global lock. Searches
// scan every shard under its read lock.
type VectorIndex struct {
	mu         sync.RWMutex // structural lock: dimensions, precision, shard maps
	dimensions int
	precision  VectorPrecision
	shards     []vecShard
	mask       uint32
}

type vecShard struct {
	mu  sync.RWMutex
	f64 map[string][]float64
	f32 map[string][]float32
	i8  map[string]QuantizedVector
}

var _ VectorStore = (*VectorIndex)(nil)

func NewVectorIndex(dimensions int) *VectorIndex {
	return NewVectorIndexWithPrecision(dimensions, PrecisionFloat32)
}

func NewVectorIndexWithPrecision(dimensions int, precision VectorPrecision) *VectorIndex {
	return NewVectorIndexWithShards(dimensions, precision, DefaultIndexShards)
}

// NewVectorIndexWithShards creates a vector index split into n lock stripes.
func NewVectorIndexWithShards(dimensions int, precision VectorPrecision, n int) *VectorIndex {
	if dimensions <= 0 {
		dimensions = 128
	}
	if precision != PrecisionInt8 && precision != PrecisionFloat64 {
		precision = PrecisionFloat32
	}
	n = normalizeShardCount(n)
	vi := &VectorIndex{
		dimensions: dimensions,
		precision:  precision,
		shards:     make([]vecShard, n),
		mask:       uint32(n - 1),
	}
	for i := range vi.shards {
		vi.shards[i].resetFor(precision)
	}
	return vi
}

func (s *vecShard) resetFor(p VectorPrecision) {
	s.f64, s.f32, s.i8 = nil, nil, nil
	switch p {
	case PrecisionInt8:
		s.i8 = make(map[string]QuantizedVector)
	case PrecisionFloat64:
		s.f64 = make(map[string][]float64)
	default:
		s.f32 = make(map[string][]float32)
	}
}

func (vi *VectorIndex) shardFor(docID string) *vecShard {
	return &vi.shards[fnv1a32(docID)&vi.mask]
}

// ShardCount returns the number of lock stripes.
func (vi *VectorIndex) ShardCount() int {
	return len(vi.shards)
}

// Len returns the number of stored vectors.
func (vi *VectorIndex) Len() int {
	if vi == nil {
		return 0
	}
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	n := 0
	for i := range vi.shards {
		s := &vi.shards[i]
		s.mu.RLock()
		n += len(s.f64) + len(s.f32) + len(s.i8)
		s.mu.RUnlock()
	}
	return n
}

func (vi *VectorIndex) Dimensions() int {
	if vi == nil {
		return 0
	}
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	return vi.dimensions
}

func (vi *VectorIndex) ProviderName() string {
	return "memory_flat"
}

func (vi *VectorIndex) Close() error {
	return nil
}

func (vi *VectorIndex) GetPrecision() VectorPrecision {
	if vi == nil {
		return PrecisionFloat32
	}
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	return vi.precision
}

// convertShard rewrites one shard's vectors into precision p. Caller holds
// vi.mu exclusively, so no shard lock is needed.
func convertShard(s *vecShard, p VectorPrecision) {
	switch p {
	case PrecisionFloat32:
		newMap := make(map[string][]float32)
		if s.f64 != nil {
			for k, v := range s.f64 {
				f32v := make([]float32, len(v))
				for i, val := range v {
					f32v[i] = float32(val)
				}
				normV, _ := NormalizeL2Float32(f32v)
				newMap[k] = normV
			}
		} else if s.i8 != nil {
			for k, qv := range s.i8 {
				newMap[k] = DequantizeVector(qv)
			}
		} else if s.f32 != nil {
			newMap = s.f32
		}
		s.f32, s.f64, s.i8 = newMap, nil, nil

	case PrecisionInt8:
		newMap := make(map[string]QuantizedVector)
		if s.f64 != nil {
			for k, v := range s.f64 {
				if qv, err := QuantizeVector(v); err == nil {
					newMap[k] = qv
				}
			}
		} else if s.f32 != nil {
			for k, v := range s.f32 {
				f64v := make([]float64, len(v))
				for i, val := range v {
					f64v[i] = float64(val)
				}
				if qv, err := QuantizeVector(f64v); err == nil {
					newMap[k] = qv
				}
			}
		} else if s.i8 != nil {
			newMap = s.i8
		}
		s.i8, s.f64, s.f32 = newMap, nil, nil

	case PrecisionFloat64:
		newMap := make(map[string][]float64)
		if s.f32 != nil {
			for k, v := range s.f32 {
				f64v := make([]float64, len(v))
				for i, val := range v {
					f64v[i] = float64(val)
				}
				normV, _ := NormalizeL2Float64(f64v)
				newMap[k] = normV
			}
		} else if s.i8 != nil {
			for k, qv := range s.i8 {
				deq := DequantizeVector(qv)
				f64v := make([]float64, len(deq))
				for i, val := range deq {
					f64v[i] = float64(val)
				}
				newMap[k] = f64v
			}
		} else if s.f64 != nil {
			newMap = s.f64
		}
		s.f64, s.f32, s.i8 = newMap, nil, nil
	}
}

func (vi *VectorIndex) SetPrecision(p VectorPrecision) error {
	if vi == nil {
		return fmt.Errorf("vector index is nil")
	}
	if p != PrecisionFloat64 && p != PrecisionFloat32 && p != PrecisionInt8 {
		return fmt.Errorf("unsupported precision %q (expected float64, float32, or int8)", p)
	}

	vi.mu.Lock()
	defer vi.mu.Unlock()

	if vi.precision == p {
		return nil
	}
	for i := range vi.shards {
		convertShard(&vi.shards[i], p)
	}
	vi.precision = p
	return nil
}

// storeLocked encodes vec in the index precision and stores it in s.
// Caller holds vi.mu (read) and s.mu (write).
func (vi *VectorIndex) storeLocked(s *vecShard, docID string, vec []float64) error {
	switch vi.precision {
	case PrecisionInt8:
		qv, err := QuantizeVector(vec)
		if err != nil {
			return err
		}
		if s.i8 == nil {
			s.i8 = make(map[string]QuantizedVector)
		}
		s.i8[docID] = qv

	case PrecisionFloat64:
		normV, err := NormalizeL2Float64(vec)
		if err != nil {
			return err
		}
		if s.f64 == nil {
			s.f64 = make(map[string][]float64)
		}
		s.f64[docID] = normV

	default: // Float32
		f32v := make([]float32, len(vec))
		for i, val := range vec {
			f32v[i] = float32(val)
		}
		normV, err := NormalizeL2Float32(f32v)
		if err != nil {
			return err
		}
		if s.f32 == nil {
			s.f32 = make(map[string][]float32)
		}
		s.f32[docID] = normV
	}
	return nil
}

func (vi *VectorIndex) AddVector(docID string, vec []float64) error {
	if vi == nil {
		return fmt.Errorf("vector index is nil")
	}
	vi.mu.RLock()
	defer vi.mu.RUnlock()

	if len(vec) != vi.dimensions {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", vi.dimensions, len(vec))
	}

	s := vi.shardFor(docID)
	s.mu.Lock()
	defer s.mu.Unlock()
	return vi.storeLocked(s, docID, vec)
}

func (vi *VectorIndex) AddVectorBatch(docIDs []string, vectors [][]float64) error {
	if vi == nil {
		return fmt.Errorf("vector index is nil")
	}
	if len(docIDs) != len(vectors) {
		return fmt.Errorf("length mismatch: %d docIDs vs %d vectors", len(docIDs), len(vectors))
	}
	if len(docIDs) == 0 {
		return nil
	}

	// The batch is all-or-nothing, so it takes the structural lock: no other
	// writer or reader can observe a half-applied batch.
	vi.mu.Lock()
	defer vi.mu.Unlock()

	for i, vec := range vectors {
		if len(vec) != vi.dimensions {
			return fmt.Errorf("vector dimension mismatch at index %d: expected %d, got %d", i, vi.dimensions, len(vec))
		}
		for j, val := range vec {
			if math.IsNaN(val) || math.IsInf(val, 0) {
				return fmt.Errorf("vector at index %d contains NaN or Inf at dimension %d", i, j)
			}
		}
	}

	for i, docID := range docIDs {
		_ = vi.storeLocked(vi.shardFor(docID), docID, vectors[i])
	}
	return nil
}

func (vi *VectorIndex) DeleteVector(docID string) {
	if vi == nil {
		return
	}
	vi.mu.RLock()
	defer vi.mu.RUnlock()

	s := vi.shardFor(docID)
	s.mu.Lock()
	delete(s.f32, docID)
	delete(s.i8, docID)
	delete(s.f64, docID)
	s.mu.Unlock()
}

// queryEncoder prepares the normalized query once for the index precision.
type queryEncoder struct {
	q32 []float32
	q64 []float64
}

func (vi *VectorIndex) encodeQuery(queryVector []float64) (queryEncoder, bool) {
	var enc queryEncoder
	switch vi.precision {
	case PrecisionFloat64:
		normQ, err := NormalizeL2Float64(queryVector)
		if err != nil {
			return enc, false
		}
		enc.q64 = normQ
	default: // Float32 and Int8 both take a float32 query
		q32 := make([]float32, len(queryVector))
		for i, val := range queryVector {
			q32[i] = float32(val)
		}
		normQ, err := NormalizeL2Float32(q32)
		if err != nil {
			return enc, false
		}
		enc.q32 = normQ
	}
	return enc, true
}

// similarityLocked returns the similarity of docID in s, if present.
// Caller holds vi.mu (read) and s.mu (read).
func (vi *VectorIndex) similarityLocked(s *vecShard, docID string, enc queryEncoder) (float64, bool) {
	switch vi.precision {
	case PrecisionInt8:
		qv, ok := s.i8[docID]
		if !ok {
			return 0, false
		}
		return float64(DotProductInt8(enc.q32, qv)), true
	case PrecisionFloat64:
		vec, ok := s.f64[docID]
		if !ok {
			return 0, false
		}
		var dot float64
		for i := 0; i < len(enc.q64); i++ {
			dot += enc.q64[i] * vec[i]
		}
		return dot, true
	default:
		vec, ok := s.f32[docID]
		if !ok {
			return 0, false
		}
		return float64(DotProductFloat32(enc.q32, vec)), true
	}
}

func roundSim(sim float64) float64 {
	return math.Round(sim*10000) / 10000
}

func sortAndTrim(results []VectorSearchResult, topK int) []VectorSearchResult {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Similarity != results[j].Similarity {
			return results[i].Similarity > results[j].Similarity
		}
		return results[i].DocID < results[j].DocID
	})
	if len(results) > topK {
		results = results[:topK]
	}
	return results
}

func (vi *VectorIndex) SearchNearest(queryVector []float64, topK int) []VectorSearchResult {
	if vi == nil {
		return nil
	}
	vi.mu.RLock()
	defer vi.mu.RUnlock()

	if len(queryVector) != vi.dimensions || topK <= 0 {
		return nil
	}
	enc, ok := vi.encodeQuery(queryVector)
	if !ok {
		return nil
	}

	var results []VectorSearchResult
	for i := range vi.shards {
		s := &vi.shards[i]
		s.mu.RLock()
		switch vi.precision {
		case PrecisionInt8:
			for docID, qv := range s.i8 {
				if sim := float64(DotProductInt8(enc.q32, qv)); sim > 0 {
					results = append(results, VectorSearchResult{DocID: docID, Similarity: roundSim(sim)})
				}
			}
		case PrecisionFloat64:
			for docID, vec := range s.f64 {
				var dot float64
				for d := 0; d < len(enc.q64); d++ {
					dot += enc.q64[d] * vec[d]
				}
				if dot > 0 {
					results = append(results, VectorSearchResult{DocID: docID, Similarity: roundSim(dot)})
				}
			}
		default:
			for docID, vec := range s.f32 {
				if sim := float64(DotProductFloat32(enc.q32, vec)); sim > 0 {
					results = append(results, VectorSearchResult{DocID: docID, Similarity: roundSim(sim)})
				}
			}
		}
		s.mu.RUnlock()
	}

	return sortAndTrim(results, topK)
}

// GetSimilarity computes the similarity score of a specific document vector against a query.
func (vi *VectorIndex) GetSimilarity(docID string, queryVector []float64) (float64, bool) {
	if vi == nil {
		return 0.0, false
	}
	vi.mu.RLock()
	defer vi.mu.RUnlock()

	if len(queryVector) != vi.dimensions {
		return 0.0, false
	}
	enc, ok := vi.encodeQuery(queryVector)
	if !ok {
		return 0.0, false
	}
	s := vi.shardFor(docID)
	s.mu.RLock()
	sim, found := vi.similarityLocked(s, docID, enc)
	s.mu.RUnlock()
	if !found {
		return 0.0, false
	}
	return roundSim(sim), true
}

// SearchSubset computes vector similarity exclusively on a specified candidate subset.
func (vi *VectorIndex) SearchSubset(queryVector []float64, candidateDocIDs []string, topK int) []VectorSearchResult {
	if vi == nil || len(candidateDocIDs) == 0 || topK <= 0 {
		return nil
	}
	vi.mu.RLock()
	defer vi.mu.RUnlock()

	if len(queryVector) != vi.dimensions {
		return nil
	}
	enc, ok := vi.encodeQuery(queryVector)
	if !ok {
		return nil
	}

	var results []VectorSearchResult
	for _, docID := range candidateDocIDs {
		s := vi.shardFor(docID)
		s.mu.RLock()
		sim, found := vi.similarityLocked(s, docID, enc)
		s.mu.RUnlock()
		if found && sim > 0 {
			results = append(results, VectorSearchResult{DocID: docID, Similarity: roundSim(sim)})
		}
	}

	return sortAndTrim(results, topK)
}

type vectorSnapshot struct {
	Dimensions       int                        `json:"dimensions"`
	Precision        VectorPrecision            `json:"precision,omitempty"`
	Vectors          map[string][]float64       `json:"vectors,omitempty"`
	F32Vectors       map[string][]float32       `json:"f32_vectors,omitempty"`
	QuantizedVectors map[string]QuantizedVector `json:"quantized_vectors,omitempty"`
}

// SaveSnapshot writes the same single-index JSON format as before sharding.
func (vi *VectorIndex) SaveSnapshot(filePath string) error {
	if vi == nil {
		return fmt.Errorf("vector index is nil")
	}
	vi.mu.RLock()
	snap := vectorSnapshot{Dimensions: vi.dimensions, Precision: vi.precision}
	for i := range vi.shards {
		s := &vi.shards[i]
		s.mu.RLock()
		if s.f64 != nil {
			if snap.Vectors == nil {
				snap.Vectors = make(map[string][]float64)
			}
			for k, v := range s.f64 {
				vecCopy := make([]float64, len(v))
				copy(vecCopy, v)
				snap.Vectors[k] = vecCopy
			}
		}
		if s.f32 != nil {
			if snap.F32Vectors == nil {
				snap.F32Vectors = make(map[string][]float32)
			}
			for k, v := range s.f32 {
				vecCopy := make([]float32, len(v))
				copy(vecCopy, v)
				snap.F32Vectors[k] = vecCopy
			}
		}
		if s.i8 != nil {
			if snap.QuantizedVectors == nil {
				snap.QuantizedVectors = make(map[string]QuantizedVector)
			}
			for k, qv := range s.i8 {
				dataCopy := make([]int8, len(qv.Data))
				copy(dataCopy, qv.Data)
				snap.QuantizedVectors[k] = QuantizedVector{Data: dataCopy, Scale: qv.Scale}
			}
		}
		s.mu.RUnlock()
	}
	vi.mu.RUnlock()

	tmpPath := filePath + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if tmpFile != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	if err := json.NewEncoder(tmpFile).Encode(snap); err != nil {
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	tmpFile = nil

	return os.Rename(tmpPath, filePath)
}

func (vi *VectorIndex) LoadSnapshot(filePath string) error {
	if vi == nil {
		return fmt.Errorf("vector index is nil")
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	var snap vectorSnapshot
	empty := len(bytes.TrimSpace(data)) == 0
	if !empty {
		if err := json.Unmarshal(data, &snap); err != nil {
			return err
		}
	}

	vi.mu.Lock()
	defer vi.mu.Unlock()

	if snap.Dimensions > 0 {
		vi.dimensions = snap.Dimensions
	}

	// Decide the precision the loaded vectors will live in.
	switch {
	case len(snap.QuantizedVectors) > 0:
		vi.precision = PrecisionInt8
	case len(snap.F32Vectors) > 0:
		vi.precision = PrecisionFloat32
	}
	for i := range vi.shards {
		vi.shards[i].resetFor(vi.precision)
	}

	switch {
	case len(snap.QuantizedVectors) > 0:
		for k, qv := range snap.QuantizedVectors {
			vi.shardFor(k).i8[k] = qv
		}
	case len(snap.F32Vectors) > 0:
		for k, v := range snap.F32Vectors {
			vi.shardFor(k).f32[k] = v
		}
	case len(snap.Vectors) > 0:
		// Legacy snapshot migration into the active precision.
		for k, v := range snap.Vectors {
			s := vi.shardFor(k)
			switch vi.precision {
			case PrecisionInt8:
				if qv, err := QuantizeVector(v); err == nil {
					s.i8[k] = qv
				}
			case PrecisionFloat64:
				s.f64[k] = v
			default:
				f32v := make([]float32, len(v))
				for i, val := range v {
					f32v[i] = float32(val)
				}
				normV, _ := NormalizeL2Float32(f32v)
				s.f32[k] = normV
			}
		}
	}

	return nil
}
