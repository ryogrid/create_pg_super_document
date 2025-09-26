# QTNodeCompare

## Location
src/backend/utils/adt/tsquery_util.c: 97 - 152

## Overview
Provides a recursive comparison function for QTNode structures, implementing a deterministic sort order for text search query tree nodes.

## Definition


## Detailed Description
QTNodeCompare is a recursive comparison function that establishes a total ordering for QTNode structures. It compares two QTNode trees and returns an integer indicating their relative order (-1, 0, or 1), following standard comparator conventions.

The comparison algorithm follows this hierarchy:
1. **Node type comparison**: Operators (QI_OPR) are ordered before values (QI_VAL)
2. **For operators**: Compares operator type, number of children, then recursively compares each child pair, and finally operator distance for phrase operators
3. **For values**: Compares CRC values first, then performs lexicographic string comparison using tsCompareString

This deterministic ordering is essential for query normalization, optimization, and duplicate detection in the text search system.

## Parameters / Member Variables
- : Pointer to the first QTNode to compare
- : Pointer to the second QTNode to compare

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - QTNodeCompare (recursive self-call)
  - tsCompareString (string comparison utility)
  - elog (error logging)
- Data types and constants used:
  - QTNode
  - QueryOperator
  - QueryOperand
  - QI_OPR (operator query item type)
  - QI_VAL (value query item type)
  - OP_PHRASE (phrase operator constant)
- Called from (representative examples):
  - CompareTSQ
  - findeq
  - cmpQTN
  - QTNEq

## Notes and Other Information
- Returns -1 if an < bn, 0 if an == bn, 1 if an > bn in the defined ordering
- The sort order is described as "somewhat arbitrary" but must be consistent and deterministic
- Includes stack depth checking to prevent overflow during deep recursion
- For phrase operators, distance is used as an additional comparison criterion
- Error handling for unrecognized QueryItem types with appropriate logging
- Essential for query tree normalization and comparison operations in PostgreSQL's full-text search