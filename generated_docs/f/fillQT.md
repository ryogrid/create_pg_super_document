# fillQT

## Location
[src/backend/utils/adt/tsquery_util.c:323-362](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_util.c#L323-L362)

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
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)  
  - [fillQT](fillQT.md) (recursive self-call)
  - memcpy (memory copying)
  - Assert (debugging assertion)
  - QTN2QTState, QTNode, QueryOperand, QueryOperator, QueryItem, QI_VAL, QI_OPR (data types and constants)
- Called from (representative examples):
  - [QTN2QT](../Q/QTN2QT.md) (in tsquery_util.c)

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

## Simplified Source

```c
static void fillQT(QTN2QTState *state, QTNode *in) {
    // Prevent stack overflow on deep recursion
    check_stack_depth();

    if (in->valnode->type == QI_VAL) {
        // Handle value nodes (leaf nodes with operands)
        memcpy(state->curitem, in->valnode, sizeof(QueryOperand));

        // Copy operand string and set up distance offset
        memcpy(state->curoperand, in->word, in->valnode->qoperand.length);
        state->curitem->qoperand.distance = state->curoperand - state->operand;
        state->curoperand[in->valnode->qoperand.length] = '\0';

        // Advance pointers for next item
        state->curoperand += in->valnode->qoperand.length + 1;
        state->curitem++;
    } else {
        // Handle operator nodes (internal nodes)
        QueryItem *curitem = state->curitem;

        memcpy(state->curitem, in->valnode, sizeof(QueryOperator));
        state->curitem++;

        // Process first child
        fillQT(state, in->child[0]);

        // Process second child for binary operators
        if (in->nchild == 2) {
            curitem->qoperator.left = state->curitem - curitem;
            fillQT(state, in->child[1]);
        }
    }
}
```