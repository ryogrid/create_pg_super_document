# bringetbitmap

## Location
src/backend/access/brin/brin.c: 558 - 947

## Overview
Executes a BRIN index scan and returns a bitmap of heap pages that match the scan keys by reading index tuples from the revmap and comparing their summary values against scan conditions.

## Definition


## Detailed Description
The bringetbitmap function is the core bitmap index scan implementation for BRIN (Block Range Index) indexes. It works by:

1. Reading index TIDs from the revmap (reverse mapping) structure
2. Obtaining index tuples pointed to by these TIDs  
3. Comparing summary values in the index tuples to scan keys
4. Adding all pages in matching ranges to the TID bitmap

For ranges that are unsummarized (marked with InvalidTID in revmap), all pages in those ranges are returned regardless of scan keys since no summary information is available.

The function processes each page range by:
- Retrieving the BRIN tuple for the range from the revmap
- If no tuple exists (unsummarized range), adding all pages in the range
- If a tuple exists, deforming it and checking if it's a placeholder tuple
- For regular tuples, comparing each indexed attribute's summary values against corresponding scan keys
- Using the attribute's consistent support procedure to determine if the range matches
- Adding qualifying page ranges to the output bitmap

## Parameters / Member Variables
- : IndexScanDesc containing scan keys, index relation, and opaque scan state
- : TIDBitmap to populate with qualifying heap page numbers

## Dependencies
- Functions called/Symbols referenced:
  - [brinGetTupleForHeapBlock](brinGetTupleForHeapBlock.md): Retrieves BRIN tuple for a given heap block
  - [brin_deform_tuple](brin_deform_tuple.md): Converts physical tuple to in-memory format
  - [brin_copy_tuple](brin_copy_tuple.md): Creates a copy of a BRIN tuple
  - [check_null_keys](../c/check_null_keys.md): Validates IS NULL/IS NOT NULL scan conditions  
  - [index_getprocinfo](../i/index_getprocinfo.md): Gets consistent support procedure for attribute
  - tbm_add_page: Adds page to TID bitmap
  - pgstat_count_index_scan: Updates index scan statistics
- Called from (representative examples):
  - [brinhandler](brinhandler.md): BRIN access method handler registration

## Notes and Other Information
- Returns an approximate count of tuples (totalpages * 10) rather than exact tuple count
- Uses a per-range memory context that is reset for each range to avoid memory leaks  
- Supports both single-key and multi-key consistent functions based on function signature
- Handles both regular scan keys and IS NULL/IS NOT NULL conditions separately
- Processes scan keys by grouping them per indexed attribute for efficient evaluation
- Empty ranges (bt_empty_range = true) are automatically excluded from results