# join_tsqueries

## Location
src/backend/utils/adt/tsquery_op.c: 33 - 53

## Overview
A static helper function that combines two TSQuery objects with a specified logical operator (AND, OR, or phrase) to create a new query tree node.

## Definition


## Detailed Description
The `join_tsqueries` function is an internal utility that creates a new query tree node (`QTNode`) representing the combination of two TSQuery objects with a logical operator. It constructs a binary tree structure where the operator becomes the parent node and the two input queries become its children. The function handles special cases like phrase operators that require distance information. This is a fundamental building block for implementing TSQuery operations like AND, OR, and phrase searches.

## Parameters / Member Variables
- `a`: First TSQuery to be combined (becomes right child in the result tree)
- `b`: Second TSQuery to be combined (becomes left child in the result tree)  
- `operator`: The logical operator to apply (OP_AND, OP_OR, OP_PHRASE, etc.)
- `distance`: Distance parameter used specifically for phrase operators (OP_PHRASE)

## Dependencies
- Functions called/Symbols referenced:
  - `[palloc0](../p/palloc0.md)` - Allocates zero-initialized memory
  - `QT2QTN` - Converts query tree to query tree node format
  - `GETQUERY` - Extracts query portion from TSQuery
  - `GETOPERAND` - Extracts operand portion from TSQuery
  - `QTNode` - [Query](../Q/Query.md) tree node structure
  - `QueryItem` - [Query](../Q/Query.md) item structure for operators
  - `QTN_NEEDFREE` - Flag indicating memory needs to be freed
  - `QI_OPR` - [Query](../Q/Query.md) item type for operators
  - `OP_PHRASE` - Phrase operator constant

- Called from (representative examples):
  - `[tsquery_and](../t/tsquery_and.md)` - For AND operations
  - `[tsquery_or](../t/tsquery_or.md)` - For OR operations  
  - `[tsquery_phrase_distance](../t/tsquery_phrase_distance.md)` - For phrase operations with distance

## Notes and Other Information
- The function creates a binary tree structure with the operator as parent and queries as children
- Child order is significant: b becomes child[0] (left), a becomes child[1] (right)
- Memory management is handled through the QTN_NEEDFREE flag
- Distance parameter is only meaningful for phrase operators
- This is a static function, only accessible within the same compilation unit