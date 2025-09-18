# CHKVAL

## Location
[src/backend/utils/adt/tsvector_op.c:42-44](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L42-L44)

## Overview
CHKVAL is a structure used as a callback parameter for text search (tsquery) operations in PostgreSQL's GiST index implementation, containing array boundaries for efficient range-based lookups.

## Definition


## Detailed Description
CHKVAL serves as a context structure passed to callback functions during text search query execution. It encapsulates array boundary pointers that define a range of elements (typically word hash values or WordEntry structures) to be searched. This structure is used in both GiST index operations for tsvector and regular tsvector matching operations.

The structure enables binary search algorithms to efficiently locate matching terms within sorted arrays by maintaining pointers to the beginning and end of the search range. It acts as a bridge between the query execution framework and the specific comparison functions that need access to the underlying data arrays.

## Parameters / Member Variables
- : Pointer to the beginning of the array being searched (array begin)
- : Pointer to the end of the array being searched (array end)

## Dependencies
- Functions called/Symbols referenced:
  - TSTernaryValue (return type for callback functions)
- Called from (representative examples):
  - [checkcondition_arr](../c/checkcondition_arr.md) (in tsgistidx.c:287-288)
  - [gtsvector_consistent](../g/gtsvector_consistent.md) (in tsgistidx.c:362)
  - [checkclass_str](../c/checkclass_str.md) (in tsvector_op.c:1189)
  - [checkcondition_str](../c/checkcondition_str.md) (in tsvector_op.c:1297)
  - [ts_match_vq](../t/ts_match_vq.md) (in tsvector_op.c:2218)

## Notes and Other Information
- This structure is designed for use with the TS_execute callback mechanism in PostgreSQL's text search infrastructure
- The array boundaries can point to different data types depending on context: int32 arrays (for GiST leaf pages) or WordEntry arrays (for tsvector operations)
- The structure supports efficient binary search algorithms by maintaining loop invariants like "StopLow <= val < StopHigh"
- Part of the GiST (Generalized Search Tree) implementation for tsvector_ops opclass in PostgreSQL's full-text search functionality