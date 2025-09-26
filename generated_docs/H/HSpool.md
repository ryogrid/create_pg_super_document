# HSpool

## Location
[src/backend/access/hash/hashsort.c:39-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashsort.c#L39-L59)

## Overview
HSpool is a structure that maintains status information for the hash index spooling and sorting phase during hash index construction.

## Definition

```c
struct HSpool
{
	Tuplesortstate *sortstate;	/* state data for tuplesort.c */
	Relation	index;

	/*
	 * We sort the hash keys based on the buckets they belong to, then by the
	 * hash values themselves, to optimize insertions onto hash pages.  The
	 * masks below are used in _hash_hashkey2bucket to determine the bucket of
	 * a given hash key.
	 */
	uint32		high_mask;
	uint32		low_mask;
	uint32		max_buckets;
};
```
## Detailed Description
The HSpool structure is a key component in PostgreSQL's hash index construction process, specifically designed to optimize the building of hash indexes through an efficient spooling and sorting mechanism. During index creation, hash keys are sorted based on their target buckets and hash values to minimize random I/O during the actual insertion phase. This structure encapsulates all the necessary state information required for this optimization process, including the underlying tuplesort state and bucket calculation parameters.

## Parameters / Member Variables
- `*sortstate`: Pointer to Tuplesortstate structure that manages the actual sorting operations via tuplesort.c
- `index`: The hash index relation being constructed
- `high_mask`: High-order mask used in bucket calculation for hash key distribution
- `low_mask`: Low-order mask used in bucket calculation for hash key distribution
- `max_buckets`: Maximum number of buckets in the hash index

## Dependencies
- Functions called/Symbols referenced:
  - [Tuplesortstate](../T/Tuplesortstate.md)
- Called from (representative examples):
  - [_h_spoolinit](../h/_h_spoolinit.md)
  - [_h_spooldestroy](../h/_h_spooldestroy.md)
  - [_h_spool](../h/_h_spool.md)
  - [_h_indexbuild](../h/_h_indexbuild.md)

## Notes and Other Information
The HSpool structure is central to hash index construction optimization. The sorting strategy implemented through this structure ensures that hash keys are processed in an order that maximizes locality during hash page insertions. The high_mask, low_mask, and max_buckets members work together with the _hash_hashkey2bucket function to determine the appropriate bucket for each hash key, enabling the sorting process to group keys by their target buckets before final insertion.