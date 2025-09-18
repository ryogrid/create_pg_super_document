# _hash_readpage

## Location
src/backend/access/hash/hashsearch.c: 446 - 601

## Overview
Loads qualifying index tuples from the current hash index page into the scan position, handling page navigation when no matches are found.

## Definition
```c
static bool _hash_readpage(IndexScanDesc scan, Buffer *bufP, ScanDirection dir)
```

## Detailed Description
This function scans the current hash index page to find tuples that satisfy the scan qualification and loads them into the scan's current position structure. It uses binary search to locate the starting position on each page based on the hash key. When no qualifying tuples are found on the current page, it automatically advances to the next or previous page depending on scan direction.

The function handles both forward and backward scan directions with different positioning logic. For forward scans, it starts from the beginning of qualifying items and processes to the end. For backward scans, it starts from the end and processes backward. The function maintains proper buffer management, keeping pins on bucket pages while releasing overflow page buffers after loading their data.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing the scan state and relation information
- `bufP`: Pointer to Buffer being processed for tuple loading
- `dir`: ScanDirection indicating forward or backward scan direction

## Dependencies
- Functions called/Symbols referenced:
  - _hash_checkpage
  - BufferGetPage
  - HashPageGetOpaque
  - BufferGetBlockNumber
  - ScanDirectionIsForward
  - _hash_binsearch
  - _hash_binsearch_last
  - _hash_load_qualified_items
  - _hash_kill_items
  - _hash_readnext
  - _hash_readprev
  - _hash_relbuf
  - LockBuffer
- Called from (representative examples):
  - _hash_next
  - _hash_first

## Notes and Other Information
The function performs page validation using _hash_checkpage to ensure proper page type. Binary search positioning differs between scan directions: forward scans use _hash_binsearch while backward scans use _hash_binsearch_last. Buffer management maintains pins on bucket pages throughout scans but releases overflow page buffers after data extraction. The function handles scrollable cursor requirements by preserving page navigation information. Return value indicates whether qualifying tuples were found and loaded successfully.