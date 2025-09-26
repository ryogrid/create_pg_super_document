# checkcondition_bit

## Location
[src/backend/utils/adt/tsgistidx.c:317-333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L317-L333)

## Overview
A callback function used in TSVector GiST indexing that checks if a query operand matches against bit signature data in non-leaf index pages using bloom filter logic.

## Definition
```c
static TSTernaryValue checkcondition_bit(void *checkval, QueryOperand *val, ExecPhraseData *data)
```

## Detailed Description
This function serves as a TS_execute callback specifically designed for matching tsquery operands against GiST non-leaf page data stored as bit signatures (bloom filters). It implements a fast bit-level check to determine if a query term might be present in the subtree represented by the signature.

The function operates on SignTSVector structures that contain bit signatures created by hashing TSVector terms. It uses the HASHVAL macro to compute a bit position from the query operand's CRC32 hash, then checks if that bit is set in the signature. This provides a probabilistic membership test - if the bit is not set, the term is definitely not present (`TS_NO`), but if the bit is set, the term may or may not be present (`TS_MAYBE`).

This approach enables efficient filtering of index subtrees during query processing, allowing the search to skip entire branches that cannot contain matching documents.

## Parameters / Member Variables
- `checkval`: Void pointer to SignTSVector structure containing the bit signature data
- `val`: QueryOperand pointer containing:
  - `valcrc`: CRC32 hash value of the query term
  - `prefix`: Boolean indicating if this is a prefix search
- `data`: ExecPhraseData pointer (unused in this function but required by callback interface)

## Dependencies
- Functions called/Symbols referenced:
  - [SignTSVector](../S/SignTSVector.md): TSVector signature structure type
  - `GETBIT`: Macro to check if a specific bit is set in the signature
  - `GETSIGN`: Macro to extract the bit signature from SignTSVector
  - `HASHVAL`: Macro to compute hash position within signature length
  - `GETSIGLEN`: Macro to get the signature length
  - `QueryOperand`: Query operand structure
  - `[ExecPhraseData](../E/ExecPhraseData.md)`: Execution phrase data structure
  - `TSTernaryValue`: Return type enum (`TS_NO`, `TS_MAYBE`, `TS_YES`)
- Called from (representative examples):
  - [gtsvector_consistent](../g/gtsvector_consistent.md): Main consistency checking function for TSVector GiST operations

## Notes and Other Information
- Implements probabilistic membership testing using bloom filter principles
- Cannot handle prefix searches due to signature-based storage (returns `TS_MAYBE` for prefixes)
- Returns only `TS_MAYBE` or `TS_NO` - never `TS_YES` due to probabilistic nature of bloom filters
- Designed for non-leaf pages in GiST tree structure for efficient subtree filtering
- Works with bit signatures created by `makesign` function during compression
- Enables quick elimination of irrelevant index branches during query processing
- Part of the two-tier TSVector GiST indexing strategy (signatures for large data, arrays for smaller data)