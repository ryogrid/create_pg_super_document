# hashrescan

## Location
src/backend/access/hash/hash.c: 397 - 430

## Overview
Resets and reinitializes an existing hash index scan with new scan keys, cleaning up any previous scan state.

## Definition
```c
void hashrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
                ScanKey orderbys, int norderbys)
```

## Detailed Description
The hashrescan function resets an existing hash index scan to start over, optionally with new search criteria. This is more efficient than ending the current scan and starting a completely new one, as it reuses the already-allocated scan structures.

The function performs several cleanup operations: it processes any killed (dead) tuples that were marked during the previous scan by calling _hash_kill_items, releases any currently held buffer pins with _hash_dropscanbuf, and invalidates the current scan position to force the next tuple fetch to restart from the beginning.

If new scan keys are provided, the function updates the scan's key data by copying the new keys over the old ones. The bucket population and split tracking flags are reset to indicate that bucket analysis needs to be performed again for the new scan.

## Parameters / Member Variables
- `scan`: IndexScanDesc structure for the existing scan to be reset
- `scankey`: New scan keys (search conditions) to use, or NULL to keep existing keys
- `nscankeys`: Number of new scan keys provided
- `orderbys`: Order by keys (unused for hash indexes, should be NULL)
- `norderbys`: Number of order by keys (should be 0 for hash indexes)

## Dependencies
- Functions called/Symbols referenced:
  - HashScanPosIsValid
  - _hash_kill_items
  - _hash_dropscanbuf
  - HashScanPosInvalidate
  - memmove (for copying scan keys)
  - ScanKeyData (structure)
- Called from (representative examples):
  - hashhandler (hash access method handler)
  - Referenced in HASHNProcs (hash index procedure array)

## Notes and Other Information
- More efficient than ending and restarting a scan from scratch
- Handles cleanup of killed tuples from the previous scan iteration
- Supports changing scan keys without recreating the entire scan structure
- Resets bucket analysis flags to handle potential index changes since the last scan
- The orderbys parameter is ignored since hash indexes do not support ordered retrieval