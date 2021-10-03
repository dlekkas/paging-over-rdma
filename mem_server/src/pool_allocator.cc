#include <stdlib.h>
#include <iostream>

#include <cstddef>


struct Chunk {
	Chunk *next;
};

/**
 * Fixed-size chunk allocator. User can invoke methods to allocate or
 * deallocate chunks of fixed-size `S`. Internally, memory is allocated
 * in the granularity of blocks and each block is comprised of `NC` chunks.
 * Chunks are also aligned with an alignment of `S` bytes.
 */
template <std::size_t NC, std::size_t S = 4096>
class FixedChunkAllocator {
 public:
	explicit FixedChunkAllocator(bool prealloc);

	~FixedChunkAllocator();

	void* ChunkAlloc();

	void ChunkRelease(void* chunk);

 private:
	constexpr static std::size_t block_sz_ = NC * S;

	std::size_t n_init_blocks_;

	// Pointer to the first chunk in the free list.
	Chunk* alloc_ptr_ = nullptr;

	// Should not be invoked if free chunks are available
	// (i.e. alloc_ptr_ != nullptr).
	Chunk* alloc_block_();
};

template <std::size_t NC, std::size_t S>
FixedChunkAllocator<NC, S>::FixedChunkAllocator(bool prealloc) {
	if (prealloc) {
		alloc_ptr_ = alloc_block_();
	}
}

// TODO(dimlek): free all the allocated memory here
template <std::size_t NC, std::size_t S>
FixedChunkAllocator<NC, S>::~FixedChunkAllocator() {
	// free mem here
}


template <std::size_t NC, std::size_t S>
void* FixedChunkAllocator<NC, S>::ChunkAlloc() {
	// if we don't have chunks available, then we allocate a new block
	if (alloc_ptr_ == nullptr) {
		alloc_ptr_ = alloc_block_();
	}

	// pick the first free chunk
	Chunk *res = alloc_ptr_;
	alloc_ptr_ = alloc_ptr_->next;

	return res;
}

template <std::size_t NC, std::size_t S>
void FixedChunkAllocator<NC, S>::ChunkRelease(void* chunk) {
	Chunk* curr = reinterpret_cast<Chunk*>(chunk);
	curr->next = alloc_ptr_;
	alloc_ptr_ = reinterpret_cast<Chunk*>(chunk);
}

template <std::size_t NC, std::size_t S>
Chunk* FixedChunkAllocator<NC, S>::alloc_block_() {
	void* buf = aligned_alloc(block_sz_, S);
	if (buf == nullptr) {
		std::cerr << "Failed to allocate " << block_sz_ << " bytes.\n";
		return nullptr;
	}

	Chunk* head_chunk = reinterpret_cast<Chunk*>(buf);

	Chunk* iter = head_chunk;
	for (std::size_t i = 0; i < NC-1; i++) {
		iter->next = reinterpret_cast<Chunk*>(reinterpret_cast<char*>(iter) + S);
		iter = iter->next;
	}
	iter->next = nullptr;

	return head_chunk;
}
