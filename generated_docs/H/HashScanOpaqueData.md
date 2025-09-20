# HashScanOpaqueData

## Location
[src/include/access/hash.h:158-190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/hash.h#L158-L190)

## Overview
HashScanOpaqueData is the private state structure for hash index scans, containing scan position, bucket information, and split operation handling data.

## Definition

```c
typedef struct HashScanOpaqueData
{
	/* Hash value of the scan key, ie, the hash key we seek */
	uint32		hashso_sk_hash;

	/* remember the buffer associated with primary bucket */
	Buffer		hashso_bucket_buf;

	/*
	 * remember the buffer associated with primary bucket page of bucket being
	 * split.  it is required during the scan of the bucket which is being
	 * populated during split operation.
	 */
	Buffer		hashso_split_bucket_buf;

	/* Whether scan starts on bucket being populated due to split */
	bool		hashso_buc_populated;

	/*
	 * Whether scanning bucket being split?  The value of this parameter is
	 * referred only when hashso_buc_populated is true.
	 */
	bool		hashso_buc_split;
	/* info about killed items if any (killedItems is NULL if never used) */
	int		   *killedItems;	/* currPos.items indexes of killed items */
	int			numKilled;		/* number of currently stored items */

	/*
	 * Identify all the matching items on a page and save them in
	 * HashScanPosData
	 */
	HashScanPosData currPos;	/* current position data */
} HashScanOpaqueData;
```
## Detailed Description
HashScanOpaqueData serves as the comprehensive state management structure for hash index scanning operations. It handles the complex scenarios that arise during bucket splitting operations, where a scan may need to access both the original bucket and the newly created bucket to ensure all relevant tuples are found.

The structure maintains buffers for both normal bucket access and split bucket scenarios, ensuring that scans remain consistent even when concurrent split operations are occurring. The killed items tracking mechanism supports PostgreSQL's MVCC system by allowing efficient cleanup of dead tuples during scan operations.

## Parameters / Member Variables
- `hashso_sk_hash`: Hash value of the scan key being searched for
- `hashso_bucket_buf`: Buffer reference for the primary bucket page
- `hashso_split_bucket_buf`: Buffer reference for the primary bucket page of a bucket currently being split (used during split operations)
- `hashso_buc_populated`: Boolean indicating whether the scan starts on a bucket being populated due to a split operation
- `hashso_buc_split`: Boolean indicating whether the scan is processing a bucket currently being split (only relevant when hashso_buc_populated is true)
- `*killedItems`: Array of indexes into currPos.items pointing to killed (dead) items, or NULL if unused
- `numKilled`: Count of currently stored killed items
- `currPos`: HashScanPosData structure containing current scan position and matched items
## Dependencies
- Functions called/Symbols referenced:
  - [HashScanPosData](HashScanPosData.md)
  - Buffer
- Called from (representative examples):
  - [hashbeginscan](../h/hashbeginscan.md)
  - HashScanOpaque

## Notes and Other Information
The split-handling logic in this structure is crucial for maintaining scan consistency during dynamic hash table operations. When a bucket split occurs during a scan, the structure ensures that all relevant tuples are found by potentially scanning both the original and new buckets. The killed items mechanism provides an optimization for dead tuple cleanup, allowing multiple dead tuples to be processed efficiently in batch operations rather than individually.