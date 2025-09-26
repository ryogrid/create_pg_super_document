# cntsize

## Location
[src/backend/utils/adt/tsquery_util.c:292-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_util.c#L292-L315)

## Overview
cntsize is a recursive helper function that counts the total length of operand strings and the total number of nodes in a QTNode tree.

## Definition
```c
static void cntsize(QTNode *in, int *sumlen, int *nnode)
```

## Detailed Description
cntsize performs a recursive tree traversal to gather size metrics needed for memory allocation when converting QTNode trees to other formats. It counts two key metrics: the total number of nodes in the tree and the cumulative length of all operand strings (including null terminators).

For operator nodes (QI_OPR type), the function recursively processes all children. For operand nodes, it adds the operand length plus 1 (for the null terminator) to the running sum. Every node visited increments the node count.

The function includes stack depth checking to prevent stack overflow during deep recursion and expects the caller to initialize the accumulator variables to zero before the first call.

## Parameters / Member Variables
- `in`: Pointer to the QTNode tree to analyze
- `sumlen`: Pointer to integer accumulator for total string length (including null terminators)
- `nnode`: Pointer to integer accumulator for total node count

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](check_stack_depth.md) (stack overflow protection)
  - [cntsize](cntsize.md) (recursive self-call)
  - [QTNode](../Q/QTNode.md), QI_OPR, QueryItem (data types and constants)
- Called from (representative examples):
  - [QTN2QT](../Q/QTN2QT.md) (in tsquery_util.c)

## Notes and Other Information
- Static function (internal to tsquery_util.c)
- Caller must initialize *sumlen and *nnode to zero before calling
- Includes null terminators in string length calculations
- Uses recursive approach with stack depth checking for safety
- Essential for pre-calculating memory requirements before tree format conversions
- Part of PostgreSQL's text search query processing utilities
- Located in src/backend/utils/adt/tsquery_util.c:292-315