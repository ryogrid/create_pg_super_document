# heapgettup

## Location
src/backend/access/heap/heapam.c: 882 - 991

## Overview
The core function for fetching the next heap tuple during sequential table scanning, handling scan initialization, tuple visibility checking, and key matching.

## Definition


## Detailed Description
This function implements the main logic for heap table scanning. If the scan is already initialized, it continues from the previously returned page/tuple using heapgettup_continue_page(). For new scans, it fetches pages using heap_fetch_next_buffer() and processes each tuple on the page. For each tuple, it checks visibility using HeapTupleSatisfiesVisibility(), handles serializable isolation conflicts, and applies scan key filters. When a qualifying tuple is found, it sets scan->rs_ctup and rs_coffset and returns. The scan state is reset when no more tuples are found.

## Parameters / Member Variables
- : HeapScanDesc - The heap scan descriptor containing scan state and configuration
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
  - heapgettup_continue_page
  - heap_fetch_next_buffer
  - heapgettup_start_page
  - LockBuffer
  - BufferGetBlockNumber
  - PageGetItemId
  - ItemIdIsNormal
  - PageGetItem
  - ItemIdGetLength
  - ItemPointerSet
  - HeapTupleSatisfiesVisibility
  - HeapCheckForSerializableConflictOut
  - HeapKeyTest
  - ReleaseBuffer
- Called from (representative examples):
  - heap_getnext
  - heap_getnextslot
  - heap_getnextslot_tidrange

## Notes and Other Information
- The function can be called with nkeys/key parameters separate from those in the scan descriptor when the caller doesn't want scan key checking
- When the scan falls off either end, rs_inited is reset, causing subsequent requests in the same direction to restart the scan
- Requests in the opposite direction after reaching scan end will start a fresh scan in the proper direction
- Buffer locking (BUFFER_LOCK_SHARE) is used during page access and released before returning
- Tuple data is set in scan->rs_ctup, with t_data set to NULL when no more tuples exist
- The function handles both continuing existing scans and initializing new scans within the same code path