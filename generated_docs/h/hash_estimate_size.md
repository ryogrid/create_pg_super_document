# hash_estimate_size

## Location
[src/backend/utils/hash/dynahash.c:784-830](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L784-L830)

## Overview
Estimates the memory footprint required for a shared memory hashtable with a given number of entries and entry size.

## Definition
```c
Size hash_estimate_size(long num_entries, Size entrysize)
```

## Detailed Description
The `hash_estimate_size` function calculates the total memory requirement for a PostgreSQL hash table that will be stored in shared memory. It provides accurate size estimates by accounting for all major components: the hash header structure, directory entries, segments, and the actual hash elements. The function assumes default values for all hash structure parameters and is specifically designed for shared memory usage estimation, excluding the local HTAB structure.

The calculation considers the dynamic growth patterns of hash tables, estimating the number of buckets as the next power of two, computing required segments and directory entries, and accounting for element allocation in groups as determined by the choose_nelem_alloc function.

## Parameters / Member Variables
- `num_entries`: Expected number of entries the hash table will contain
- `entrysize`: Size in bytes of each hash table entry

## Dependencies
- Functions called/Symbols referenced:
  - [next_pow2_long](../n/next_pow2_long.md) (calculates next power of two for bucket and segment sizing)
  - [choose_nelem_alloc](../c/choose_nelem_alloc.md) (determines optimal element allocation group size)
  - [add_size](../a/add_size.md), mul_size (safe arithmetic operations for size calculations)
  - MAXALIGN (aligns sizes to proper boundaries)
- Called from (representative examples):
  - [BufTableShmemSize](../B/BufTableShmemSize.md) (buffer table shared memory sizing)
  - [LockShmemSize](../L/LockShmemSize.md) (lock manager shared memory sizing)
  - [CalculateShmemSize](../C/CalculateShmemSize.md) (overall shared memory calculation)
  - [PredicateLockShmemSize](../P/PredicateLockShmemSize.md) (predicate lock shared memory sizing)

## Notes and Other Information
- Designed specifically for shared memory hashtables - does not count local HTAB structure
- Assumes all hash structure parameters have default values (DEF_SEGSIZE, DEF_DIRSIZE)
- Uses safe arithmetic functions to prevent overflow in size calculations
- Elements are allocated in groups, not individually, for efficiency
- Critical for PostgreSQL shared memory management and sizing decisions
- All size calculations include proper alignment requirements

## Simplified Source

```c
// Simplified version of hash_estimate_size
Size hash_estimate_size(long num_entries, Size entrysize) {
    Size total_size;
    long num_buckets, num_segments, num_dir_entries;
    long element_alloc_count, element_size, element_allocations;

    // Step 1: Calculate bucket count (next power of 2)
    num_buckets = next_pow2_long(num_entries);

    // Step 2: Calculate segments needed for buckets
    num_segments = next_pow2_long((num_buckets - 1) / DEF_SEGSIZE + 1);

    // Step 3: Calculate directory entries (grows by doubling)
    num_dir_entries = DEF_DIRSIZE;
    while (num_dir_entries < num_segments) {
        num_dir_entries <<= 1;
    }

    // Step 4: Calculate total size components
    // Hash header structure
    total_size = MAXALIGN(sizeof(HASHHDR));

    // Directory space
    total_size = add_size(total_size,
                         mul_size(num_dir_entries, sizeof(HASHSEGMENT)));

    // Segment space
    total_size = add_size(total_size,
                         mul_size(num_segments,
                                 MAXALIGN(DEF_SEGSIZE * sizeof(HASHBUCKET))));

    // Step 5: Calculate element storage space
    element_alloc_count = choose_nelem_alloc(entrysize);
    element_allocations = (num_entries - 1) / element_alloc_count + 1;
    element_size = MAXALIGN(sizeof(HASHELEMENT)) + MAXALIGN(entrysize);

    total_size = add_size(total_size,
                         mul_size(element_allocations,
                                 mul_size(element_alloc_count, element_size)));

    return total_size;
}
```

Key simplifications made:
- Added descriptive variable names for clarity
- Broken down calculation into logical steps with comments
- Preserved all essential calculations and logic flow
- Maintained the exact algorithm while improving readability
- Kept all important mathematical operations and alignment requirements