# join_tsqueries

## Location
[src/backend/utils/adt/tsquery_op.c:33-53](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_op.c#L33-L53)

## Overview
A static helper function that combines two TSQuery objects with a specified logical operator (AND, OR, or phrase) to create a new query tree node.

## Definition

```c
static QTNode *
join_tsqueries(TSQuery a, TSQuery b, int8 operator, uint16 distance)
```
## Detailed Description
The `join_tsqueries` function is an internal utility that creates a new query tree node (`QTNode`) representing the combination of two TSQuery objects with a logical operator. It constructs a binary tree structure where the operator becomes the parent node and the two input queries become its children. The function handles special cases like phrase operators that require distance information. This is a fundamental building block for implementing TSQuery operations like AND, OR, and phrase searches.

## Parameters / Member Variables
- `a`: First TSQuery to be combined (becomes right child in the result tree)
- `b`: Second TSQuery to be combined (becomes left child in the result tree)  
- `operator`: The logical operator to apply (OP_AND, OP_OR, OP_PHRASE, etc.)
- `distance`: Distance parameter used specifically for phrase operators (OP_PHRASE)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) - Allocates zero-initialized memory
  - `[QT2QTN](../Q/QT2QTN.md)` - Converts query tree to query tree node format
  - `GETQUERY` - Extracts query portion from TSQuery
  - `GETOPERAND` - Extracts operand portion from TSQuery
  - `[QTNode](../Q/QTNode.md)` - [Query](../Q/Query.md) tree node structure
  - `QueryItem` - [Query](../Q/Query.md) item structure for operators
  - `QTN_NEEDFREE` - Flag indicating memory needs to be freed
  - `QI_OPR` - [Query](../Q/Query.md) item type for operators
  - `OP_PHRASE` - Phrase operator constant

- Called from (representative examples):
  - [tsquery_and](../t/tsquery_and.md) - For AND operations
  - [tsquery_or](../t/tsquery_or.md) - For OR operations  
  - [tsquery_phrase_distance](../t/tsquery_phrase_distance.md) - For phrase operations with distance

## Notes and Other Information
- The function creates a binary tree structure with the operator as parent and queries as children
- Child order is significant: b becomes child[0] (left), a becomes child[1] (right)
- Memory management is handled through the QTN_NEEDFREE flag
- Distance parameter is only meaningful for phrase operators
- This is a static function, only accessible within the same compilation unit

## Simplified Source

```c
static QTNode *join_tsqueries(TSQuery a, TSQuery b, int8 operator, uint16 distance)
{
    QTNode *result = (QTNode *) palloc0(sizeof(QTNode));

    // Mark for memory cleanup
    result->flags |= QTN_NEEDFREE;

    // Create operator node
    result->valnode = (QueryItem *) palloc0(sizeof(QueryItem));
    result->valnode->type = QI_OPR;
    result->valnode->qoperator.oper = operator;

    // Set distance for phrase operators
    if (operator == OP_PHRASE)
        result->valnode->qoperator.distance = distance;

    // Create child nodes (b=left, a=right)
    result->child = (QTNode **) palloc0(sizeof(QTNode *) * 2);
    result->child[0] = QT2QTN(GETQUERY(b), GETOPERAND(b));
    result->child[1] = QT2QTN(GETQUERY(a), GETOPERAND(a));
    result->nchild = 2;

    return result;
}
```