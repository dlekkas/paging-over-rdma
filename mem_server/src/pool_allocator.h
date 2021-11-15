#include <stdlib.h>
#include <iostream>
#include <cstdlib>
#include <chrono>
#include <unordered_map>

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

	void BufRelease(void* buf);
	void ChunkRelease(void* chunk);

	uint64_t ChunkResolve(void* buf);

	std::unordered_map<uint64_t, uint64_t> remap;

 private:
	constexpr static std::size_t block_sz_ = NC * S;

	constexpr static std::size_t n_batches_ = 64;

	constexpr static std::size_t batch_sz_ = n_batches_ * S;

	int resolve_shift_bits;

	// Pointer to the first chunk in the free list.
	Chunk* alloc_ptr_ = nullptr;

	// Should not be invoked if free chunks are available
	// (i.e. alloc_ptr_ != nullptr).
	Chunk* alloc_block_();
};

template <std::size_t NC, std::size_t S>
FixedChunkAllocator<NC, S>::FixedChunkAllocator(bool prealloc) {
	resolve_shift_bits = __builtin_ctz(batch_sz_);
	if (prealloc) {
		alloc_ptr_ = alloc_block_();
	}
}

// TODO(dimlek): free all the allocated memory here
template <std::size_t NC, std::size_t S>
FixedChunkAllocator<NC, S>::~FixedChunkAllocator() {
	// free mem here
}

template<std::size_t NC, std::size_t S>
uint64_t FixedChunkAllocator<NC, S>::ChunkResolve(void* buf) {
	return (uint64_t) buf >> resolve_shift_bits;
}

template <std::size_t NC, std::size_t S>
Chunk* FixedChunkAllocator<NC, S>::alloc_block_() {
	//TODO(dimlek): find out why aligned alloc fails
	void* buf = std::aligned_alloc(batch_sz_, block_sz_);
	//void* buf = malloc(block_sz_);
	if (buf == nullptr) {
		std::cerr << "Failed to allocate " << block_sz_ << " bytes.\n";
		return nullptr;
	}
	Chunk* head_chunk = reinterpret_cast<Chunk*>(buf);

	Chunk* iter = head_chunk;
	for (std::size_t i = 0; i < NC-1; i++) {
		if (i % n_batches_ == 0) {
			iter->next = reinterpret_cast<Chunk*>((uint64_t) iter + batch_sz_);
			iter = iter->next;
			//remap[ChunkResolve(iter)] = 0;
		}
	}
	iter->next = nullptr;

	return head_chunk;
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
void FixedChunkAllocator<NC, S>::BufRelease(void* buf) {
	remap[ChunkResolve(buf)]++;
	if (remap[ChunkResolve(buf)] == n_batches_) {
		ChunkRelease(ChunkResolve(buf));
	}
}

template <std::size_t NC, std::size_t S>
void FixedChunkAllocator<NC, S>::ChunkRelease(void* chunk) {
	Chunk* curr = reinterpret_cast<Chunk*>(chunk);
	curr->next = alloc_ptr_;
	alloc_ptr_ = reinterpret_cast<Chunk*>(chunk);
}

