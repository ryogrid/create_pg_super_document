# heapgettup

## Location
[src/backend/access/heap/heapam.c:882-991](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L882-L991)

## Overview
The core function for fetching the next heap tuple during sequential table scanning, handling scan initialization, tuple visibility checking, and key matching.

## Definition

```c
static void
heapgettup(HeapScanDesc scan,
		   ScanDirection dir,
		   int nkeys,
		   ScanKey key)
```
## Detailed Description
This function implements the main logic for heap table scanning. If the scan is already initialized, it continues from the previously returned page/tuple using heapgettup_continue_page(). For new scans, it fetches pages using heap_fetch_next_buffer() and processes each tuple on the page. For each tuple, it checks visibility using HeapTupleSatisfiesVisibility(), handles serializable isolation conflicts, and applies scan key filters. When a qualifying tuple is found, it sets scan->rs_ctup and rs_coffset and returns. The scan state is reset when no more tuples are found.

## Parameters / Member Variables
- : HeapScanDesc - The heap scan descriptor containing scan state and configuration
README.md				filter_frequent_symbol_from_csv.py
__pycache__				global_symbols.db
area					import_symbol_reference.py
attnums					output
base.nKeys				process_symbol_definitions.py
contrib					scripts
create_duckdb_index.py			set_file_end_lines.py
data					src
extract_readme_file_header_comments.py	update_symbol_types.py: ScanDirection - Direction of the scan (forward or backward)
- : int - Number of scan keys for filtering tuples
- : ScanKey - Array of scan keys used to filter tuples (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [heapgettup_continue_page](heapgettup_continue_page.md)
  - [heap_fetch_next_buffer](heap_fetch_next_buffer.md)
  - [heapgettup_start_page](heapgettup_start_page.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsNormal
  - [PageGetItem](../P/PageGetItem.md)
  - ItemIdGetLength
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - [HeapTupleSatisfiesVisibility](../H/HeapTupleSatisfiesVisibility.md)
  - [HeapCheckForSerializableConflictOut](../H/HeapCheckForSerializableConflictOut.md)
  - [HeapKeyTest](../H/HeapKeyTest.md)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
- Called from (representative examples):
  - [heap_getnext](heap_getnext.md)
  - [heap_getnextslot](heap_getnextslot.md)
  - [heap_getnextslot_tidrange](heap_getnextslot_tidrange.md)

## Notes and Other Information
- The function can be called with nkeys/key parameters separate from those in the scan descriptor when the caller doesn't want scan key checking
- When the scan falls off either end, rs_inited is reset, causing subsequent requests in the same direction to restart the scan
- Requests in the opposite direction after reaching scan end will start a fresh scan in the proper direction
- Buffer locking (BUFFER_LOCK_SHARE) is used during page access and released before returning
- Tuple data is set in scan->rs_ctup, with t_data set to NULL when no more tuples exist
- The function handles both continuing existing scans and initializing new scans within the same code path

## Simplified Source

```c
// Simplified version of heapgettup - fetch next heap tuple
static void
heapgettup(HeapScanDesc scan, ScanDirection dir, int nkeys, ScanKey key)
{
    HeapTuple tuple = &(scan->rs_ctup);
    Page page;
    OffsetNumber lineoff;
    int linesleft;

    // If scan already initialized, continue from current position
    if (scan->rs_inited) {
        LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);
        page = heapgettup_continue_page(scan, dir, &linesleft, &lineoff);
        goto scan_page;
    }

    // Main scan loop - advance through pages until we find a qualifying tuple
    while (true) {
        // Get next buffer/page to scan
        heap_fetch_next_buffer(scan, dir);

        // Check if we've run out of pages
        if (!BufferIsValid(scan->rs_cbuf))
            break;

        // Lock page and start scanning from beginning
        LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);
        page = heapgettup_start_page(scan, dir, &linesleft, &lineoff);

scan_page:
        // Scan all tuples on current page
        for (; linesleft > 0; linesleft--, lineoff += dir) {
            ItemId lpp = PageGetItemId(page, lineoff);

            // Skip invalid line pointers
            if (!ItemIdIsNormal(lpp))
                continue;

            // Set up tuple data from page item
            tuple->t_data = (HeapTupleHeader) PageGetItem(page, lpp);
            tuple->t_len = ItemIdGetLength(lpp);
            ItemPointerSet(&(tuple->t_self), scan->rs_cblock, lineoff);

            // Check if tuple is visible to our snapshot
            bool visible = HeapTupleSatisfiesVisibility(tuple,
                                                       scan->rs_base.rs_snapshot,
                                                       scan->rs_cbuf);

            // Handle serializable isolation conflicts
            HeapCheckForSerializableConflictOut(visible, scan->rs_base.rs_rd,
                                              tuple, scan->rs_cbuf,
                                              scan->rs_base.rs_snapshot);

            // Skip invisible tuples
            if (!visible)
                continue;

            // Apply scan key filters if provided
            if (key != NULL &&
                !HeapKeyTest(tuple, RelationGetDescr(scan->rs_base.rs_rd), nkeys, key))
                continue;

            // Found qualifying tuple - unlock and return
            LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
            scan->rs_coffset = lineoff;
            return;
        }

        // Finished page, unlock and continue to next
        LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
    }

    // End of scan - clean up and reset state
    if (BufferIsValid(scan->rs_cbuf))
        ReleaseBuffer(scan->rs_cbuf);

    scan->rs_cbuf = InvalidBuffer;
    scan->rs_cblock = InvalidBlockNumber;
    scan->rs_prefetch_block = InvalidBlockNumber;
    tuple->t_data = NULL;
    scan->rs_inited = false;
}
```

Key simplifications made:
- Added descriptive comments explaining each major section
- Simplified variable declarations and removed some intermediate assignments
- Clarified the two-phase logic: continue existing scan vs. start new scan
- Used more descriptive goto label name (`scan_page` instead of `continue_page`)
- Consolidated buffer validity and cleanup logic
- Added inline comments for complex operations like visibility checking
- Removed detailed assertions and focused on core algorithm flow
- Maintained all essential logic while improving readability