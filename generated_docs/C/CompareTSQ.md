# CompareTSQ

## Location
[src/backend/utils/adt/tsquery_op.c:189-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_op.c#L189-L214)

## Overview
Compares two TSQuery objects for ordering, providing a complete comparison function that considers size, memory footprint, and structural content.

## Definition

```c
static int
CompareTSQ(TSQuery a, TSQuery b)
```
## Detailed Description
The  function implements a comprehensive comparison between two TSQuery objects, returning an integer indicating their relative ordering (-1, 0, or 1). The comparison follows a hierarchical approach: first comparing the number of query items (size), then the total memory size (VARSIZE), and finally performing a deep structural comparison of the query trees themselves.

For non-empty queries, the function converts both TSQuery objects to their corresponding QTNode tree representations and delegates the detailed comparison to . This ensures that queries with identical logical structure are considered equal regardless of their serialized representation.

The function is static, indicating it's used internally within the tsquery_op.c module for comparison operations and sorting.

## Parameters / Member Variables
- : First TSQuery object to compare
- : Second TSQuery object to compare

## Dependencies
- Functions called/Symbols referenced:
  - VARSIZE (macro for getting variable-length object size)
  - [QT2QTN](../Q/QT2QTN.md) (converts TSQuery to QTNode tree)
  - GETQUERY (gets query part from TSQuery)
  - GETOPERAND (gets operand part from TSQuery)
  - [QTNodeCompare](../Q/QTNodeCompare.md) (compares QTNode tree structures)
  - [QTNFree](../Q/QTNFree.md) (frees QTNode tree memory)
- Called from:
  - [tsquery_cmp](../t/tsquery_cmp.md)
  - CMPFUNC (comparison macro usage)

## Notes and Other Information
- Returns -1 if a < b, 0 if a == b, 1 if a > b
- Comparison precedence: size → memory footprint → structural content
- Empty queries (size == 0) are considered equal regardless of their memory layout
- Properly manages temporary QTNode allocations by freeing them after comparison
- Used as the foundation for tsquery equality and ordering operations
- Static function, not exposed in the public API