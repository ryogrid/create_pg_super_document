# fillQT

## Location
src/backend/utils/adt/tsquery_util.c: 323 - 362

## Overview
fillQT is a recursive helper function that converts a QTNode tree into flat tsquery format by filling pre-allocated arrays with the tree's contents in the appropriate binary representation.

## Definition
```c
static void fillQT(QTN2QTState *state, QTNode *in)
```

## Detailed Description
fillQT performs a recursive traversal of a QTNode tree to populate flat arrays that represent a TSQuery in PostgreSQL's internal binary format. The function handles two types of nodes differently:

For value nodes (QI_VAL), it copies the QueryOperand structure to the current item position and copies the operand string to the operand buffer, setting up proper distance offsets and null termination.

For operator nodes (QI_OPR), it copies the QueryOperator structure and recursively processes children. For binary operators, it sets the left offset to indicate the distance to the right operand before processing the second child.

The function expects pre-allocated arrays of correct size and uses a state structure to track current positions in these arrays during the conversion process.

## Parameters / Member Variables
- `state`: Pointer to QTN2QTState structure containing current positions and target arrays
- `in`: Pointer to the QTNode tree to convert

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)  
  - fillQT (recursive self-call)
  - memcpy (memory copying)
  - Assert (debugging assertion)
  - QTN2QTState, QTNode, QueryOperand, QueryOperator, QueryItem, QI_VAL, QI_OPR (data types and constants)
- Called from (representative examples):
  - QTN2QT (in tsquery_util.c)

## Notes and Other Information
- Static function (internal to tsquery_util.c)
- Assumes binary tree structure (at most 2 children per operator node)
- Caller must pre-allocate arrays of correct size using cntsize results
- Handles proper offset calculation for binary operators in flat representation
- Includes null termination for operand strings
- Uses recursive approach with stack depth checking for safety
- Essential component of QTNode to TSQuery conversion process
- Part of PostgreSQL's text search query processing utilities
- Located in src/backend/utils/adt/tsquery_util.c:323-362