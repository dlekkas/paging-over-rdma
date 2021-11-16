#include <stdlib.h>
#include <iostream>
#include <functional>
#include <optional>
#include <utility>
#include <cstdlib>
#include <cerrno>
#include <chrono>
#include <unordered_map>
#include <infiniband/verbs.h>

#include <cstddef>

static int global_cnt = 0;

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
	FixedChunkAllocator(struct ibv_pd* pd, bool prealloc);

	~FixedChunkAllocator();

	std::pair<void*,int> ChunkAlloc();

	void BufRelease(void* buf);
	void ChunkRelease(void* chunk);

	uint64_t ChunkResolve(void* buf);
	uint64_t BlockResolve(void* buf);

	std::unordered_map<uint64_t, uint64_t> mr_map;
	std::unordered_map<uint64_t, uint64_t> remap;

 private:
	constexpr static std::size_t block_sz_ = NC * S;

	constexpr static std::size_t n_batches_ = 64;

	constexpr static std::size_t batch_sz_ = n_batches_ * S;

	int chunk_resolve_shift_bits_;
	int block_resolve_shift_bits_;

	struct ibv_pd* pd_;
	struct ibv_mr* mr_;

	// Pointer to the first chunk in the free list.
	Chunk* alloc_ptr_ = nullptr;

	// Should not be invoked if free chunks are available
	// (i.e. alloc_ptr_ != nullptr).
	Chunk* alloc_block_();

};

template <std::size_t NC, std::size_t S>
FixedChunkAllocator<NC, S>::FixedChunkAllocator(struct ibv_pd* pd,
		bool prealloc) {
	chunk_resolve_shift_bits_ = __builtin_ctz(batch_sz_);
	block_resolve_shift_bits_ = __builtin_ctz(block_sz_);
	pd_ = pd;
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
	return (uint64_t) buf >> chunk_resolve_shift_bits_;
}

template<std::size_t NC, std::size_t S>
uint64_t FixedChunkAllocator<NC, S>::BlockResolve(void* buf) {
	return (uint64_t) buf >> block_resolve_shift_bits_;
}

template <std::size_t NC, std::size_t S>
Chunk* FixedChunkAllocator<NC, S>::alloc_block_() {
	global_cnt++;
	void* buf = std::aligned_alloc(block_sz_, block_sz_);
	if (buf == nullptr) {
		std::cerr << "Failed to allocate " << block_sz_ << " bytes.\n";
		return nullptr;
	}
	Chunk* head_chunk = reinterpret_cast<Chunk*>(buf);

	Chunk* iter = head_chunk;

	int n_iters = NC / n_batches_;
	for (std::size_t i = 0; i < n_iters-1; i++) {
		iter->next = reinterpret_cast<Chunk*>((uint64_t) iter + batch_sz_);
		iter = iter->next;
		remap[ChunkResolve(iter)] = 0;
		mr_map[BlockResolve(iter)] = global_cnt;
	}
	iter->next = nullptr;

	/*
	mr_ = ibv_reg_mr(pd_, buf, block_sz_, IBV_ACCESS_LOCAL_WRITE |
			IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
	if (!mr_) {
		std::cerr << "ibv_reg_mr failed: " << std::strerror(errno) << "\n";
		return nullptr;
	}
	*/

	return head_chunk;
}

template <std::size_t NC, std::size_t S>
std::pair<void*,int> FixedChunkAllocator<NC, S>::ChunkAlloc() {
	// if we don't have chunks available, then we allocate a new block
	if (alloc_ptr_ == nullptr) {
		alloc_ptr_ = alloc_block_();
	}

	// pick the first free chunk
	Chunk *res = alloc_ptr_;
	alloc_ptr_ = alloc_ptr_->next;

	return std::make_pair(res, mr_map[BlockResolve(res)]);
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
