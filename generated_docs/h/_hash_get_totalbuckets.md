# _hash_get_totalbuckets

## Location
[src/backend/access/hash/hashutil.c:174-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashutil.c#L174-L209)

## Overview
Calculates and returns the total number of buckets allocated up to a given splitpoint phase in PostgreSQL's hash index structure.

## Definition

```c
uint32
_hash_get_totalbuckets(uint32 splitpoint_phase)
```
## Detailed Description
This function computes the total number of buckets that have been allocated in a hash index up to and including a specified splitpoint phase. The hash index uses a dynamic bucket allocation strategy where buckets are added in phases during index growth. The function handles two distinct cases:

1. **Early phases**: For splitpoint phases less than , it uses a simple power-of-2 calculation ()
2. **Later phases**: For higher phases, it uses a more complex calculation that accounts for splitpoint groups and phases within groups

The algorithm determines which splitpoint group the phase belongs to, calculates buckets from previous groups, and adds buckets from the current group based on the number of phases completed within that group.

## Parameters
- : The splitpoint phase number for which to calculate total buckets

## Dependencies
- Functions called/Symbols referenced:
  - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE (constant)
  - HASH_SPLITPOINT_PHASE_BITS (constant)
  - HASH_SPLITPOINT_PHASE_MASK (constant)
- Called from (representative examples):
  - bitno_to_blkno
  - _hash_ovflblkno_to_bitno
  - _hash_init_metabuffer
  - _hash_expandtable

## Notes and Other Information
This function is critical for hash index maintenance and expansion operations. It provides the foundation for determining bucket numbering and allocation strategies. The splitpoint mechanism allows hash indexes to grow incrementally without requiring complete reorganization, making this function essential for performance during dynamic index growth.