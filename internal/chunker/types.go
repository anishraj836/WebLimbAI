package chunker

type Chunk struct {
	ID          string
	DocURL      string
	ChunkIndex  int
	Breadcrumb  string
	Content     string
	Tokens      int
	StartOffset int
	EndOffset   int
}

type ChunkingOptions struct {
	MaxTokens          int
	Overlap            int
	PreserveCodeBlocks bool
}

func DefaultChunkingOptions() ChunkingOptions {
	return ChunkingOptions{
		MaxTokens:          512,
		Overlap:            64,
		PreserveCodeBlocks: true,
	}
}
