# QTN2QT

## Location
[src/backend/utils/adt/tsquery_util.c:363-395](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_util.c#L363-L395)

## Overview
Converts a QTNode tree representation into a flat TSQuery structure for efficient storage and processing.

## Definition

```c
TSQuery
QTN2QT(QTNode *in)
```
## Detailed Description
QTN2QT transforms a tree-based query representation (QTNode) into a flattened TSQuery format. This conversion is essential for tsquery operations as it creates the compact binary representation used throughout PostgreSQL's text search system. The function performs size calculations, validates query limits, allocates memory for the result, and then fills the flat structure using a state-based approach.

## Parameters / Member Variables
- `*in`: QTNode tree structure representing the parsed tsquery that needs to be converted to flat format
## Dependencies
- Functions called/Symbols referenced:
  - [cntsize](../c/cntsize.md) (calculates total size and node count)
  - TSQUERY_TOO_BIG (macro to check size limits)
  - COMPUTESIZE (calculates required memory size)
  - [palloc0](../p/palloc0.md) (allocates zero-initialized memory)
  - SET_VARSIZE (sets variable-length structure size)
  - GETQUERY (gets query item array from TSQuery)
  - GETOPERAND (gets operand data from TSQuery)
  - [fillQT](../f/fillQT.md) (fills the flat structure from tree)
- Called from (representative examples):
  - [tsquery_and](../t/tsquery_and.md) (logical AND operations)
  - [tsquery_or](../t/tsquery_or.md) (logical OR operations)
  - [tsquery_phrase_distance](../t/tsquery_phrase_distance.md) (phrase distance operations)
  - [tsquery_not](../t/tsquery_not.md) (logical NOT operations)
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md) (query rewriting)

## Notes and Other Information
- Raises ERROR with ERRCODE_PROGRAM_LIMIT_EXCEEDED if the tsquery exceeds size limits
- Uses QTN2QTState structure to track current position during flat structure creation
- The resulting TSQuery uses a compact binary format for efficient storage and processing
- Critical function in tsquery processing pipeline, converting parsed trees to executable format

## Simplified Source

```c
TSQuery QTN2QT(QTNode *in) {
    TSQuery out;
    int len;
    int sumlen = 0, nnode = 0;
    QTN2QTState state;

    // Calculate total size and node count from tree
    cntsize(in, &sumlen, &nnode);

    // Check if query exceeds maximum allowed size
    if (TSQUERY_TOO_BIG(nnode, sumlen))
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg("tsquery is too large")));

    // Allocate memory for flat TSQuery structure
    len = COMPUTESIZE(nnode, sumlen);
    out = (TSQuery) palloc0(len);
    SET_VARSIZE(out, len);
    out->size = nnode;

    // Initialize state for tree-to-flat conversion
    state.curitem = GETQUERY(out);
    state.operand = state.curoperand = GETOPERAND(out);

    // Convert tree structure to flat representation
    fillQT(&state, in);
    return out;
}
```