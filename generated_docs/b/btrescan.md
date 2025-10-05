# btrescan

## Location
[src/backend/access/nbtree/nbtree.c:359-416](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtree.c#L359-L416)

## Overview
Resets and prepares a B-tree index scan with new scan keys, handling cleanup of previous scan state and initializing workspace for index-only scans when needed.

## Definition

```c
structure also makes it safe to return data from a
	 * "name" column, even though btree name_ops uses an underlying storage
	 * datatype of cstring.  The risk there is that "name" is supposed to be
	 * padded to NAMEDATALEN, but the actual index tuple is probably shorter.
	 * However, since we only return data out of tuples sitting in the
	 * currTuples array, a fetch of NAMEDATALEN bytes can at worst pull some
	 * data out of the markTuples array --- running off the end of memory for
	 * a SIGSEGV is not possible.  Yeah, this is ugly as sin, but it beats
	 * adding special-case treatment for name_ops elsewhere.
	 */
	if (scan->xs_want_itup && so->currTuples == NULL)
	{
		so->currTuples = (char *) palloc(BLCKSZ * 2);
		so->markTuples = so->currTuples + BLCKSZ;
	}

	/*
	 * Reset the scan keys
	 */
	if (scankey && scan->numberOfKeys > 0)
		memmove(scan->keyData,
				scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));
```
## Detailed Description
The btrescan function reinitializes an existing B-tree index scan with new scan parameters. It performs cleanup operations including handling killed items from the previous scan, unpinning buffer pages, and invalidating scan positions. The function also allocates tuple workspace arrays for index-only scans if needed, using a single memory block for both current and mark tuple workspaces for efficiency. This function is called both for initial scan setup and when restarting a scan with different keys.

## Parameters / Member Variables
- : The IndexScanDesc structure representing the ongoing scan
- : Array of scan keys defining the scan conditions  
- : Number of scan keys in the scankey array
- : Array of order-by keys (unused for B-tree, should be NULL)
- : Number of order-by keys (should be 0 for B-tree)

## Dependencies
- Functions called/Symbols referenced:
  - BTScanPosIsValid
  - [_bt_killitems](_bt_killitems.md)
  - BTScanPosUnpinIfPinned
  - BTScanPosInvalidate
  - [IndexScanDesc](../I/IndexScanDesc.md)
  - ScanKey
  - BTScanOpaque
- Called from (representative examples):
  - [bthandler](bthandler.md)

## Notes and Other Information
- Handles cleanup of killed items from previous scan iterations using _bt_killitems
- Allocates tuple workspace as a single BLCKSZ*2 block, with markTuples positioned after currTuples
- The tuple workspace allocation includes a safety mechanism for name_ops columns to prevent memory access violations
- Resets scan position markers and key counts, which will be properly set later by _bt_preprocess_keys
- Only allocates tuple workspace when xs_want_itup is true, indicating an index-only scan is desired

## Simplified Source

```c
void btrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
             ScanKey orderbys, int norderbys) {
    BTScanOpaque so = (BTScanOpaque) scan->opaque;

    // Clean up current scan position
    if (BTScanPosIsValid(so->currPos)) {
        // Process any killed items before leaving page
        if (so->numKilled > 0)
            _bt_killitems(scan);
        BTScanPosUnpinIfPinned(so->currPos);
        BTScanPosInvalidate(so->currPos);
    }

    // Reset scan state
    so->markItemIndex = -1;
    so->needPrimScan = false;
    so->scanBehind = false;
    BTScanPosUnpinIfPinned(so->markPos);
    BTScanPosInvalidate(so->markPos);

    // Allocate tuple workspace for index-only scans if needed
    if (scan->xs_want_itup && so->currTuples == NULL) {
        so->currTuples = (char *) palloc(BLCKSZ * 2);
        so->markTuples = so->currTuples + BLCKSZ;  // Second half of allocation
    }

    // Copy new scan keys
    if (scankey && scan->numberOfKeys > 0) {
        memmove(scan->keyData, scankey,
                scan->numberOfKeys * sizeof(ScanKeyData));
    }

    // Reset key counts (will be set by _bt_preprocess_keys)
    so->numberOfKeys = 0;
    so->numArrayKeys = 0;
}
```