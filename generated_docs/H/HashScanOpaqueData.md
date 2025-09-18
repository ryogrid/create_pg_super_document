# HashScanOpaqueData

## Location
src/include/access/hash.h: 158 - 190

## Overview
HashScanOpaqueData is the private state structure for hash index scans, containing scan position, bucket information, and split operation handling data.

## Definition


## Detailed Description
HashScanOpaqueData serves as the comprehensive state management structure for hash index scanning operations. It handles the complex scenarios that arise during bucket splitting operations, where a scan may need to access both the original bucket and the newly created bucket to ensure all relevant tuples are found.

The structure maintains buffers for both normal bucket access and split bucket scenarios, ensuring that scans remain consistent even when concurrent split operations are occurring. The killed items tracking mechanism supports PostgreSQL's MVCC system by allowing efficient cleanup of dead tuples during scan operations.

## Parameters / Member Variables
- : Hash value of the scan key being searched for
- : Buffer reference for the primary bucket page
- : Buffer reference for the primary bucket page of a bucket currently being split (used during split operations)
- : Boolean indicating whether the scan starts on a bucket being populated due to a split operation
- : Boolean indicating whether the scan is processing a bucket currently being split (only relevant when hashso_buc_populated is true)
- : Array of indexes into currPos.items pointing to killed (dead) items, or NULL if unused
- : Count of currently stored killed items
- : HashScanPosData structure containing current scan position and matched items

## Dependencies
- Functions called/Symbols referenced:
  - [HashScanPosData](HashScanPosData.md)
  - Buffer
- Called from (representative examples):
  - [hashbeginscan](../h/hashbeginscan.md)
  - HashScanOpaque

## Notes and Other Information
The split-handling logic in this structure is crucial for maintaining scan consistency during dynamic hash table operations. When a bucket split occurs during a scan, the structure ensures that all relevant tuples are found by potentially scanning both the original and new buckets. The killed items mechanism provides an optimization for dead tuple cleanup, allowing multiple dead tuples to be processed efficiently in batch operations rather than individually.