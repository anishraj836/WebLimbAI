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

type VectorIndex struct {
	mu         sync.RWMutex
	dimensions int
	precision  VectorPrecision
	f64Vectors map[string][]float64
	f32Vectors map[string][]float32
	i8Vectors  map[string]QuantizedVector
}

var _ VectorStore = (*VectorIndex)(nil)

func NewVectorIndex(dimensions int) *VectorIndex {
	return NewVectorIndexWithPrecision(dimensions, PrecisionFloat32)
}

func NewVectorIndexWithPrecision(dimensions int, precision VectorPrecision) *VectorIndex {
	if dimensions <= 0 {
		dimensions = 128
	}
	if precision == "" {
		precision = PrecisionFloat32
	}

	vi := &VectorIndex{
		dimensions: dimensions,
		precision:  precision,
	}

	switch precision {
	case PrecisionInt8:
		vi.i8Vectors = make(map[string]QuantizedVector)
	case PrecisionFloat64:
		vi.f64Vectors = make(map[string][]float64)
	default:
		vi.precision = PrecisionFloat32
		vi.f32Vectors = make(map[string][]float32)
	}

	return vi
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

	// Convert all vectors from current active map to target precision map
	switch p {
	case PrecisionFloat32:
		newMap := make(map[string][]float32)
		if vi.f64Vectors != nil {
			for k, v := range vi.f64Vectors {
				f32v := make([]float32, len(v))
				for i, val := range v {
					f32v[i] = float32(val)
				}
				normV, _ := NormalizeL2Float32(f32v)
				newMap[k] = normV
			}
		} else if vi.i8Vectors != nil {
			for k, qv := range vi.i8Vectors {
				newMap[k] = DequantizeVector(qv)
			}
		}
		vi.f32Vectors = newMap
		vi.f64Vectors = nil
		vi.i8Vectors = nil

	case PrecisionInt8:
		newMap := make(map[string]QuantizedVector)
		if vi.f64Vectors != nil {
			for k, v := range vi.f64Vectors {
				qv, err := QuantizeVector(v)
				if err == nil {
					newMap[k] = qv
				}
			}
		} else if vi.f32Vectors != nil {
			for k, v := range vi.f32Vectors {
				f64v := make([]float64, len(v))
				for i, val := range v {
					f64v[i] = float64(val)
				}
				qv, err := QuantizeVector(f64v)
				if err == nil {
					newMap[k] = qv
				}
			}
		}
		vi.i8Vectors = newMap
		vi.f64Vectors = nil
		vi.f32Vectors = nil

	case PrecisionFloat64:
		newMap := make(map[string][]float64)
		if vi.f32Vectors != nil {
			for k, v := range vi.f32Vectors {
				f64v := make([]float64, len(v))
				for i, val := range v {
					f64v[i] = float64(val)
				}
				normV, _ := NormalizeL2Float64(f64v)
				newMap[k] = normV
			}
		} else if vi.i8Vectors != nil {
			for k, qv := range vi.i8Vectors {
				deq := DequantizeVector(qv)
				f64v := make([]float64, len(deq))
				for i, val := range deq {
					f64v[i] = float64(val)
				}
				newMap[k] = f64v
			}
		}
		vi.f64Vectors = newMap
		vi.f32Vectors = nil
		vi.i8Vectors = nil
	}

	vi.precision = p
	return nil
}

func (vi *VectorIndex) AddVector(docID string, vec []float64) error {
	if vi == nil {
		return fmt.Errorf("vector index is nil")
	}
	vi.mu.Lock()
	defer vi.mu.Unlock()

	if len(vec) != vi.dimensions {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", vi.dimensions, len(vec))
	}

	switch vi.precision {
	case PrecisionInt8:
		qv, err := QuantizeVector(vec)
		if err != nil {
			return err
		}
		if vi.i8Vectors == nil {
			vi.i8Vectors = make(map[string]QuantizedVector)
		}
		vi.i8Vectors[docID] = qv

	case PrecisionFloat64:
		normV, err := NormalizeL2Float64(vec)
		if err != nil {
			return err
		}
		if vi.f64Vectors == nil {
			vi.f64Vectors = make(map[string][]float64)
		}
		vi.f64Vectors[docID] = normV

	default: // Float32
		f32v := make([]float32, len(vec))
		for i, val := range vec {
			f32v[i] = float32(val)
		}
		normV, err := NormalizeL2Float32(f32v)
		if err != nil {
			return err
		}
		if vi.f32Vectors == nil {
			vi.f32Vectors = make(map[string][]float32)
		}
		vi.f32Vectors[docID] = normV
	}

	return nil
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

	vi.mu.Lock()
	defer vi.mu.Unlock()

	// Atomic length, dimension, and NaN pre-validation
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

	switch vi.precision {
	case PrecisionInt8:
		if vi.i8Vectors == nil {
			vi.i8Vectors = make(map[string]QuantizedVector)
		}
		for i, docID := range docIDs {
			qv, _ := QuantizeVector(vectors[i])
			vi.i8Vectors[docID] = qv
		}

	case PrecisionFloat64:
		if vi.f64Vectors == nil {
			vi.f64Vectors = make(map[string][]float64)
		}
		for i, docID := range docIDs {
			normV, _ := NormalizeL2Float64(vectors[i])
			vi.f64Vectors[docID] = normV
		}

	default: // Float32
		if vi.f32Vectors == nil {
			vi.f32Vectors = make(map[string][]float32)
		}
		for i, docID := range docIDs {
			f32v := make([]float32, len(vectors[i]))
			for d, val := range vectors[i] {
				f32v[d] = float32(val)
			}
			normV, _ := NormalizeL2Float32(f32v)
			vi.f32Vectors[docID] = normV
		}
	}

	return nil
}

func (vi *VectorIndex) DeleteVector(docID string) {
	if vi == nil {
		return
	}
	vi.mu.Lock()
	defer vi.mu.Unlock()

	if vi.f32Vectors != nil {
		delete(vi.f32Vectors, docID)
	}
	if vi.i8Vectors != nil {
		delete(vi.i8Vectors, docID)
	}
	if vi.f64Vectors != nil {
		delete(vi.f64Vectors, docID)
	}
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

	var results []VectorSearchResult

	switch vi.precision {
	case PrecisionInt8:
		q32 := make([]float32, len(queryVector))
		for i, val := range queryVector {
			q32[i] = float32(val)
		}
		normQ, err := NormalizeL2Float32(q32)
		if err != nil {
			return nil
		}

		for docID, qv := range vi.i8Vectors {
			sim := DotProductInt8(normQ, qv)
			if sim > 0 {
				results = append(results, VectorSearchResult{
					DocID:      docID,
					Similarity: math.Round(float64(sim)*10000) / 10000,
				})
			}
		}

	case PrecisionFloat64:
		normQ, err := NormalizeL2Float64(queryVector)
		if err != nil {
			return nil
		}

		for docID, vec := range vi.f64Vectors {
			var dot float64
			for i := 0; i < len(normQ); i++ {
				dot += normQ[i] * vec[i]
			}
			if dot > 0 {
				results = append(results, VectorSearchResult{
					DocID:      docID,
					Similarity: math.Round(dot*10000) / 10000,
				})
			}
		}

	default: // Float32
		q32 := make([]float32, len(queryVector))
		for i, val := range queryVector {
			q32[i] = float32(val)
		}
		normQ, err := NormalizeL2Float32(q32)
		if err != nil {
			return nil
		}

		for docID, vec := range vi.f32Vectors {
			sim := DotProductFloat32(normQ, vec)
			if sim > 0 {
				results = append(results, VectorSearchResult{
					DocID:      docID,
					Similarity: math.Round(float64(sim)*10000) / 10000,
				})
			}
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Similarity > results[j].Similarity
	})

	if len(results) > topK {
		results = results[:topK]
	}

	return results
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

	switch vi.precision {
	case PrecisionInt8:
		qv, exists := vi.i8Vectors[docID]
		if !exists {
			return 0.0, false
		}
		q32 := make([]float32, len(queryVector))
		for i, val := range queryVector {
			q32[i] = float32(val)
		}
		normQ, err := NormalizeL2Float32(q32)
		if err != nil {
			return 0.0, false
		}
		sim := DotProductInt8(normQ, qv)
		return math.Round(float64(sim)*10000) / 10000, true

	case PrecisionFloat64:
		vec, exists := vi.f64Vectors[docID]
		if !exists {
			return 0.0, false
		}
		normQ, err := NormalizeL2Float64(queryVector)
		if err != nil {
			return 0.0, false
		}
		var dot float64
		for i := 0; i < len(normQ); i++ {
			dot += normQ[i] * vec[i]
		}
		return math.Round(dot*10000) / 10000, true

	default: // Float32
		vec, exists := vi.f32Vectors[docID]
		if !exists {
			return 0.0, false
		}
		q32 := make([]float32, len(queryVector))
		for i, val := range queryVector {
			q32[i] = float32(val)
		}
		normQ, err := NormalizeL2Float32(q32)
		if err != nil {
			return 0.0, false
		}
		sim := DotProductFloat32(normQ, vec)
		return math.Round(float64(sim)*10000) / 10000, true
	}
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

	var results []VectorSearchResult

	switch vi.precision {
	case PrecisionInt8:
		q32 := make([]float32, len(queryVector))
		for i, val := range queryVector {
			q32[i] = float32(val)
		}
		normQ, err := NormalizeL2Float32(q32)
		if err != nil {
			return nil
		}

		for _, docID := range candidateDocIDs {
			if qv, exists := vi.i8Vectors[docID]; exists {
				sim := DotProductInt8(normQ, qv)
				if sim > 0 {
					results = append(results, VectorSearchResult{
						DocID:      docID,
						Similarity: math.Round(float64(sim)*10000) / 10000,
					})
				}
			}
		}

	case PrecisionFloat64:
		normQ, err := NormalizeL2Float64(queryVector)
		if err != nil {
			return nil
		}

		for _, docID := range candidateDocIDs {
			if vec, exists := vi.f64Vectors[docID]; exists {
				var dot float64
				for i := 0; i < len(normQ); i++ {
					dot += normQ[i] * vec[i]
				}
				if dot > 0 {
					results = append(results, VectorSearchResult{
						DocID:      docID,
						Similarity: math.Round(dot*10000) / 10000,
					})
				}
			}
		}

	default: // Float32
		q32 := make([]float32, len(queryVector))
		for i, val := range queryVector {
			q32[i] = float32(val)
		}
		normQ, err := NormalizeL2Float32(q32)
		if err != nil {
			return nil
		}

		for _, docID := range candidateDocIDs {
			if vec, exists := vi.f32Vectors[docID]; exists {
				sim := DotProductFloat32(normQ, vec)
				if sim > 0 {
					results = append(results, VectorSearchResult{
						DocID:      docID,
						Similarity: math.Round(float64(sim)*10000) / 10000,
					})
				}
			}
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Similarity > results[j].Similarity
	})

	if len(results) > topK {
		results = results[:topK]
	}

	return results
}

type vectorSnapshot struct {
	Dimensions       int                        `json:"dimensions"`
	Precision        VectorPrecision            `json:"precision,omitempty"`
	Vectors          map[string][]float64       `json:"vectors,omitempty"`
	F32Vectors       map[string][]float32       `json:"f32_vectors,omitempty"`
	QuantizedVectors map[string]QuantizedVector `json:"quantized_vectors,omitempty"`
}

func (vi *VectorIndex) SaveSnapshot(filePath string) error {
	if vi == nil {
		return fmt.Errorf("vector index is nil")
	}
	vi.mu.RLock()
	dimensions := vi.dimensions
	precision := vi.precision

	var f64Copy map[string][]float64
	var f32Copy map[string][]float32
	var i8Copy map[string]QuantizedVector

	if vi.f64Vectors != nil {
		f64Copy = make(map[string][]float64, len(vi.f64Vectors))
		for k, v := range vi.f64Vectors {
			vecCopy := make([]float64, len(v))
			copy(vecCopy, v)
			f64Copy[k] = vecCopy
		}
	}
	if vi.f32Vectors != nil {
		f32Copy = make(map[string][]float32, len(vi.f32Vectors))
		for k, v := range vi.f32Vectors {
			vecCopy := make([]float32, len(v))
			copy(vecCopy, v)
			f32Copy[k] = vecCopy
		}
	}
	if vi.i8Vectors != nil {
		i8Copy = make(map[string]QuantizedVector, len(vi.i8Vectors))
		for k, qv := range vi.i8Vectors {
			dataCopy := make([]int8, len(qv.Data))
			copy(dataCopy, qv.Data)
			i8Copy[k] = QuantizedVector{
				Data:  dataCopy,
				Scale: qv.Scale,
			}
		}
	}
	vi.mu.RUnlock()

	snap := vectorSnapshot{
		Dimensions:       dimensions,
		Precision:        precision,
		Vectors:          f64Copy,
		F32Vectors:       f32Copy,
		QuantizedVectors: i8Copy,
	}

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

	vi.mu.Lock()
	defer vi.mu.Unlock()

	if len(bytes.TrimSpace(data)) == 0 {
		switch vi.precision {
		case PrecisionInt8:
			vi.i8Vectors = make(map[string]QuantizedVector)
			vi.f32Vectors = nil
			vi.f64Vectors = nil
		case PrecisionFloat64:
			vi.f64Vectors = make(map[string][]float64)
			vi.f32Vectors = nil
			vi.i8Vectors = nil
		default:
			vi.f32Vectors = make(map[string][]float32)
			vi.f64Vectors = nil
			vi.i8Vectors = nil
		}
		return nil
	}

	var snap vectorSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	if snap.Dimensions > 0 {
		vi.dimensions = snap.Dimensions
	}

	// Ingest vectors into active precision
	if snap.QuantizedVectors != nil && len(snap.QuantizedVectors) > 0 {
		vi.i8Vectors = snap.QuantizedVectors
		vi.precision = PrecisionInt8
		vi.f32Vectors = nil
		vi.f64Vectors = nil
	} else if snap.F32Vectors != nil && len(snap.F32Vectors) > 0 {
		vi.f32Vectors = snap.F32Vectors
		vi.precision = PrecisionFloat32
		vi.f64Vectors = nil
		vi.i8Vectors = nil
	} else if snap.Vectors != nil && len(snap.Vectors) > 0 {
		// Legacy snapshot migration
		switch vi.precision {
		case PrecisionInt8:
			vi.i8Vectors = make(map[string]QuantizedVector, len(snap.Vectors))
			for k, v := range snap.Vectors {
				qv, err := QuantizeVector(v)
				if err == nil {
					vi.i8Vectors[k] = qv
				}
			}
			vi.f32Vectors = nil
			vi.f64Vectors = nil
		case PrecisionFloat64:
			vi.f64Vectors = snap.Vectors
			vi.f32Vectors = nil
			vi.i8Vectors = nil
		default:
			vi.f32Vectors = make(map[string][]float32, len(snap.Vectors))
			for k, v := range snap.Vectors {
				f32v := make([]float32, len(v))
				for i, val := range v {
					f32v[i] = float32(val)
				}
				normV, _ := NormalizeL2Float32(f32v)
				vi.f32Vectors[k] = normV
			}
			vi.f64Vectors = nil
			vi.i8Vectors = nil
		}
	} else {
		switch vi.precision {
		case PrecisionInt8:
			vi.i8Vectors = make(map[string]QuantizedVector)
		case PrecisionFloat64:
			vi.f64Vectors = make(map[string][]float64)
		default:
			vi.f32Vectors = make(map[string][]float32)
		}
	}

	return nil
}
