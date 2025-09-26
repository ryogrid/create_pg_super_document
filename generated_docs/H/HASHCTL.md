# HASHCTL

## Location
src/include/utils/hsearch.h: 65 - 89

## Overview
HASHCTL is a parameter structure used to configure hash table creation in PostgreSQL, allowing customization of various aspects like partitioning, memory allocation, hash functions, and sizing parameters.

## Definition

```c
typedef struct HASHCTL
{
	/* Used if HASH_PARTITION flag is set: */
	long		num_partitions; /* # partitions (must be power of 2) */
	/* Used if HASH_SEGMENT flag is set: */
	long		ssize;			/* segment size */
	/* Used if HASH_DIRSIZE flag is set: */
	long		dsize;			/* (initial) directory size */
	long		max_dsize;		/* limit to dsize if dir size is limited */
	/* Used if HASH_ELEM flag is set (which is now required): */
	Size		keysize;		/* hash key length in bytes */
	Size		entrysize;		/* total user element size in bytes */
	/* Used if HASH_FUNCTION flag is set: */
	HashValueFunc hash;			/* hash function */
	/* Used if HASH_COMPARE flag is set: */
	HashCompareFunc match;		/* key comparison function */
	/* Used if HASH_KEYCOPY flag is set: */
	HashCopyFunc keycopy;		/* key copying function */
	/* Used if HASH_ALLOC flag is set: */
	HashAllocFunc alloc;		/* memory allocator */
	/* Used if HASH_CONTEXT flag is set: */
	MemoryContext hcxt;			/* memory context to use for allocations */
	/* Used if HASH_SHARED_MEM flag is set: */
	HASHHDR    *hctl;			/* location of header in shared mem */
} HASHCTL;
```
## Detailed Description
HASHCTL serves as the configuration structure for hash_create(), allowing callers to specify various parameters that control hash table behavior and characteristics. Each field is only used if the corresponding flag is set in the hash_flags parameter. This design provides flexibility while maintaining backward compatibility and avoiding the need to specify unused parameters.

The structure supports configuration of memory management (custom allocators, memory contexts), performance tuning (partitioning, segment sizes), custom hash functions and comparison operations, and shared memory hash tables. The HASH_ELEM flag is now required, making keysize and entrysize mandatory parameters.

## Parameters / Member Variables
- : Number of hash table partitions for concurrent access (must be power of 2)
- : Segment size for hash table storage organization
- : Initial directory size for hash table structure
- : Maximum directory size limit when directory growth is constrained
- : Size in bytes of the hash key (required with HASH_ELEM flag)
- : Total size in bytes of user data elements (required with HASH_ELEM flag)
- hash: hash table empty: Custom hash function for key hashing (HashValueFunc type)
- : Custom key comparison function for equality testing (HashCompareFunc type)
- : Custom key copying function for key duplication (HashCopyFunc type)
- : Custom memory allocation function (HashAllocFunc type)
- : Memory context for hash table allocations
- : Pointer to hash header location in shared memory

## Dependencies
- Functions called/Symbols referenced:
  - HASHHDR
- Called from (representative examples):
  - hash_create
  - hash_get_shared_size
  - init_missing_cache
  - gistInitParentMap
  - InitBufTable
  - ShmemInitHash
  - InitLocks

## Notes and Other Information
- Only fields corresponding to set hash_flags need to be initialized
- HASH_ELEM flag is now required, making keysize and entrysize mandatory
- Used extensively throughout PostgreSQL for creating hash tables with specific requirements
- Enables both regular and shared memory hash table creation
- Supports custom memory allocation strategies and hash functions
- The structure design allows for extensibility while maintaining API compatibility