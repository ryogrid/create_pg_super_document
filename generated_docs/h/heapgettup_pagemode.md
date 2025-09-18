# heapgettup_pagemode

## Location
src/backend/access/heap/heapam.c: 992 - 1081

## Overview
A specialized version of heapgettup that operates in page-at-a-time mode, processing only pre-identified visible tuples from rs_vistuples[] instead of examining all tuples on a page.

## Definition


## Detailed Description
This function provides the same API as heapgettup but operates in page-at-a-time mode for improved performance. Key differences include: no buffer content locking (handled by heap_prepare_pagescan), iteration through only the tuples listed in rs_vistuples[] rather than all page tuples, and use of 0-based lineindex instead of 1-based lineoff. The function calls heap_prepare_pagescan to prune the page and determine visible tuple offsets, then iterates through the pre-filtered visible tuples, applying scan key filters before returning qualifying tuples.

## Parameters / Member Variables
- : HeapScanDesc - The heap scan descriptor containing scan state and visible tuple information
- Pfdebug					extract_symbol_references.py
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
  - HeapKeyTest
  - ReleaseBuffer
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
- Scan state reset behavior is identical to regular heapgettup when reaching scan end