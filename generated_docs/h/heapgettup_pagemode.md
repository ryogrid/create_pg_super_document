# heapgettup_pagemode

## Location
[src/backend/access/heap/heapam.c:992-1081](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L992-L1081)

## Overview
A specialized version of heapgettup that operates in page-at-a-time mode, processing only pre-identified visible tuples from rs_vistuples[] instead of examining all tuples on a page.

## Definition

```c
static void
heapgettup_pagemode(HeapScanDesc scan,
					ScanDirection dir,
					int nkeys,
					ScanKey key)
```
## Detailed Description
This function provides the same API as heapgettup but operates in page-at-a-time mode for improved performance. Key differences include: no buffer content locking (handled by heap_prepare_pagescan), iteration through only the tuples listed in rs_vistuples[] rather than all page tuples, and use of 0-based lineindex instead of 1-based lineoff. The function calls heap_prepare_pagescan to prune the page and determine visible tuple offsets, then iterates through the pre-filtered visible tuples, applying scan key filters before returning qualifying tuples.

## Parameters / Member Variables
- : HeapScanDesc - The heap scan descriptor containing scan state and visible tuple information
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
  - [BufferGetPage](../B/BufferGetPage.md)
  - ScanDirectionIsForward
  - [heap_fetch_next_buffer](heap_fetch_next_buffer.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [heap_prepare_pagescan](heap_prepare_pagescan.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsNormal
  - [PageGetItem](../P/PageGetItem.md)
  - ItemIdGetLength
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - [HeapKeyTest](../H/HeapKeyTest.md)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
- Called from (representative examples):
  - [heap_getnext](heap_getnext.md)
  - [heap_getnextslot](heap_getnextslot.md)
  - [heap_getnextslot_tidrange](heap_getnextslot_tidrange.md)

## Notes and Other Information
- Uses rs_cindex to track position within the rs_vistuples array instead of page offsets
- Buffer locking is not required as heap_prepare_pagescan handles necessary locking
- lineindex is 0-based (vs 1-based lineoff in regular heapgettup)
- Assumes ItemIdIsNormal() for all tuples in rs_vistuples[] since they were pre-validated
- More efficient than regular heapgettup when many tuples on a page are not visible
- Still performs scan key filtering even though visibility is pre-determined
- [Scan](../S/Scan.md) state reset behavior is identical to regular heapgettup when reaching scan end

## Simplified Source

```c
// Simplified version of heapgettup_pagemode
static void heapgettup_pagemode(HeapScanDesc scan, ScanDirection dir, int nkeys, ScanKey key) {
    HeapTuple tuple = &(scan->rs_ctup);
    Page page;
    int lineindex, linesleft;

    // Continue from previously returned page/tuple if scan is already initialized
    if (scan->rs_inited) {
        page = BufferGetPage(scan->rs_cbuf);
        lineindex = scan->rs_cindex + dir;
        linesleft = ScanDirectionIsForward(dir) ?
                   (scan->rs_ntuples - lineindex) : scan->rs_cindex;
        goto continue_page;
    }

    // Main scan loop: advance through pages until we find qualifying tuple
    while (true) {
        // Get next buffer in scan direction
        heap_fetch_next_buffer(scan, dir);

        // Check if we've run out of blocks to scan
        if (!BufferIsValid(scan->rs_cbuf))
            break;

        // Prepare page for scanning - prune and determine visible tuples
        heap_prepare_pagescan((TableScanDesc) scan);
        page = BufferGetPage(scan->rs_cbuf);
        linesleft = scan->rs_ntuples;
        lineindex = ScanDirectionIsForward(dir) ? 0 : linesleft - 1;

continue_page:
        // Iterate through visible tuples on current page
        for (; linesleft > 0; linesleft--, lineindex += dir) {
            OffsetNumber lineoff = scan->rs_vistuples[lineindex];
            ItemId lpp = PageGetItemId(page, lineoff);

            // Set up tuple from page data
            tuple->t_data = (HeapTupleHeader) PageGetItem(page, lpp);
            tuple->t_len = ItemIdGetLength(lpp);
            ItemPointerSet(&(tuple->t_self), scan->rs_cblock, lineoff);

            // Apply scan key filters if provided
            if (key != NULL &&
                !HeapKeyTest(tuple, RelationGetDescr(scan->rs_base.rs_rd), nkeys, key))
                continue;

            // Found qualifying tuple - update scan position and return
            scan->rs_cindex = lineindex;
            return;
        }
    }

    // End of scan cleanup
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
- Condensed variable declarations and initialization
- Simplified conditional logic for scan direction handling
- Reduced detailed comments to focus on core algorithm steps
- Consolidated tuple setup operations into logical groups
- Streamlined end-of-scan cleanup sequence
- Preserved essential error checking and buffer management
- Maintained the critical goto label for page continuation logic