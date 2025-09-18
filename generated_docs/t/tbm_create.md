# tbm_create

## Location
src/backend/nodes/tidbitmap.c: 266 - 291

## Overview
Creates an initially empty TID (Tuple Identifier) bitmap structure that will be used to efficiently store and manage sets of tuple identifiers for bitmap heap scans.

## Definition


## Detailed Description
The  function initializes a new TIDBitmap structure in the current memory context. This bitmap is designed to efficiently store tuple identifiers (TIDs) for use in bitmap heap scans, which are an optimization technique in PostgreSQL's query execution. The function sets up the basic structure with memory management constraints and optional shared memory support through a DSA (Dynamic Shared Area).

The created bitmap starts in an empty state and can grow up to the specified memory limit. If a DSA is provided, the underlying page table storage will use shared memory, enabling parallel query execution scenarios.

## Parameters / Member Variables
- : Maximum memory consumption limit for the bitmap (approximately)
- : Optional Dynamic Shared Area for shared memory allocation; pass NULL for local memory allocation

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - tbm_calculate_entries
  - TIDBitmap (struct type)
  - TBM_EMPTY (enum value)
  - InvalidDsaPointer (constant)
  - dsa_area (struct type)
- Called from (representative examples):
  - collectMatchBitmap
  - MultiExecBitmapIndexScan
  - MultiExecBitmapOr

## Notes and Other Information
- The bitmap lives in the memory context that is current at the time of the call
- Memory allocation for page table elements uses DSA if provided, enabling shared access across processes
- The maxbytes parameter is used to calculate maximum entries via tbm_calculate_entries()
- All DSA-related pointers are initialized to InvalidDsaPointer when no DSA is provided
- The bitmap starts in TBM_EMPTY status, indicating no tuples have been added yet