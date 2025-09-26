# HashMemoryChunkData

## Location
src/include/executor/hashjoin.h: 128 - 146

## Overview
HashMemoryChunkData is a memory management structure that optimizes tuple storage in PostgreSQL hash joins by packing multiple HashJoinTuples into large 32KB buffers to reduce palloc overhead and improve memory allocation efficiency.

## Definition

```c
typedef struct HashMemoryChunkData
{
	int			ntuples;		/* number of tuples stored in this chunk */
	size_t		maxlen;			/* size of the chunk's tuple buffer */
	size_t		used;			/* number of buffer bytes already used */

	/* pointer to the next chunk (linked list) */
	union
	{
		struct HashMemoryChunkData *unshared;
		dsa_pointer shared;
	}			next;

	/*
	 * The chunk's tuple buffer starts after the HashMemoryChunkData struct,
	 * at offset HASH_CHUNK_HEADER_SIZE (which must be maxaligned).  Note that
	 * that offset is not included in "maxlen" or "used".
	 */
}			HashMemoryChunkData;
```
## Detailed Description
HashMemoryChunkData implements a chunked memory allocation strategy to optimize the storage of hash join tuples. Instead of individually allocating memory for each HashJoinTuple (which would create significant palloc overhead), this structure batches multiple tuples into large, fixed-size buffers (typically 32KB).

The structure manages both the metadata about the chunk (number of tuples, buffer size, usage tracking) and serves as the header for the actual tuple data buffer that immediately follows in memory. This design minimizes memory fragmentation and reduces the number of memory allocation calls during hash table construction.

Like other hash join structures, it supports both shared and unshared memory configurations through the union in the  field, enabling efficient operation in both single-process and parallel hash join scenarios. The chunks are organized as a linked list, allowing dynamic expansion of storage as needed during hash table construction.

## Parameters / Member Variables
- : Count of HashJoinTuple structures currently stored within this memory chunk
- : Total size in bytes of the tuple buffer portion (excluding the header), representing the maximum storage capacity
- : Number of bytes currently consumed within the tuple buffer, tracking space utilization
- : Union containing the link to the next memory chunk in the linked list
  - : Direct pointer to the next HashMemoryChunkData structure for single-process joins
  - : DSA (Dynamic Shared Area) pointer for accessing the next chunk in parallel hash joins

## Dependencies
- Functions called/Symbols referenced:
  - dsa_pointer (for parallel hash join support)
  - HashMemoryChunkData (self-reference for linked list structure)
- Called from (representative examples):
  - HashMemoryChunk (typedef alias for pointer to this structure)
  - HASH_CHUNK_HEADER_SIZE (macro calculating the header size offset)

## Notes and Other Information
- The tuple buffer begins immediately after the structure at offset HASH_CHUNK_HEADER_SIZE, which must be properly aligned (MAXALIGN)
- The header size (HASH_CHUNK_HEADER_SIZE) is not included in the maxlen or used calculations, only the tuple data portion
- Typical chunk size is 32KB to balance memory efficiency with allocation overhead
- This chunked approach significantly reduces the number of palloc/pfree operations during hash table construction and destruction
- Memory chunks are allocated from the appropriate hash join memory context (HashTableContext or batch-specific contexts)
- The structure supports both growing (adding tuples) and cleanup (releasing entire chunks) operations efficiently
- Chunks may not be fully utilized, allowing for some internal fragmentation in exchange for reduced allocation overhead