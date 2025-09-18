# heapgettup_continue_page

## Location
src/backend/access/heap/heapam.c: 752 - 797

## Overview
A helper function for heapgettup() that determines the next page to scan and calculates the remaining tuples and starting offset for scanning within that page.

## Definition


## Detailed Description
This function operates on the current buffer (rs_cbuf) of an active heap scan and prepares the scanning parameters for continuing within the same page. It handles both forward and backward scan directions, calculating the appropriate starting offset and remaining tuple count based on the scan direction and the current position (rs_coffset). For backward scans, it includes special handling to re-establish offset bounds when tuples may have been vacuumed between scans using non-MVCC snapshots.

## Parameters / Member Variables
- : HeapScanDesc - The heap scan descriptor containing the current scan state
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
- : int* - Output parameter set to the number of tuples remaining to scan on this page
- : OffsetNumber* - Output parameter set to the next offset to scan on this page

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - ScanDirectionIsForward
  - OffsetNumberNext
  - OffsetNumberPrev
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - Min
- Called from (representative examples):
  - [heapgettup](heapgettup.md)

## Notes and Other Information
- The function assumes the scan is already initialized (rs_inited) and has a valid current buffer (rs_cbuf)
- The caller is responsible for ensuring the buffer is locked if needed
- For backward scans, special handling ensures the offset doesn't exceed the page's maximum offset number, accounting for potential tuple deletions since the last scan
- This is an inline static function optimized for performance in heap scanning operations