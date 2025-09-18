# checkcondition_arr

## Location
src/backend/utils/adt/tsgistidx.c: 285 - 316

## Overview
A callback function used in TSVector GiST indexing that performs binary search to check if a query operand matches against an array of hash values in leaf-page data.

## Definition
```c
static TSTernaryValue checkcondition_arr(void *checkval, QueryOperand *val, ExecPhraseData *data)
```

## Detailed Description
This function serves as a TS_execute callback specifically designed for matching tsquery operands against GiST leaf-page data stored as sorted arrays of hash values. It implements a binary search algorithm to efficiently locate whether a query operand's CRC32 hash exists in the compressed TSVector representation.

The function operates on arrays of 32-bit hash values that were generated during the compression phase by `gtsvector_compress`. Since the arrays are maintained in sorted order, binary search provides O(log n) lookup performance.

When a match is found, the function returns `TS_MAYBE` rather than `TS_YES` because hash-based matching can produce false positives due to hash collisions. The `TS_MAYBE` result indicates that further verification is needed at a higher level to confirm the actual match.

## Parameters / Member Variables
- `checkval`: Void pointer to CHKVAL structure containing:
  - `arrb`: Pointer to the beginning of the sorted hash array
  - `arre`: Pointer to the end of the sorted hash array
- `val`: QueryOperand pointer containing:
  - `valcrc`: CRC32 hash value of the query term
  - `prefix`: Boolean indicating if this is a prefix search
- `data`: ExecPhraseData pointer (unused in this function but required by callback interface)

## Dependencies
- Functions called/Symbols referenced:
  - [CHKVAL](../C/CHKVAL.md): Structure type for array bounds
  - `QueryOperand`: Query operand structure
  - `ExecPhraseData`: Execution phrase data structure
  - `TSTernaryValue`: Return type enum (`TS_NO`, `TS_MAYBE`, `TS_YES`)
- Called from (representative examples):
  - [gtsvector_consistent](../g/gtsvector_consistent.md): Main consistency checking function for TSVector GiST operations

## Notes and Other Information
- Implements efficient O(log n) binary search for hash lookup
- Cannot handle prefix searches due to hash-based storage (returns `TS_MAYBE` for prefixes)
- Returns `TS_MAYBE` instead of `TS_YES` to account for potential hash collisions
- Part of the TSVector GiST index support infrastructure for full-text search
- Works in conjunction with the compression algorithms that create sorted hash arrays
- The binary search maintains the loop invariant: `StopLow <= val < StopHigh`