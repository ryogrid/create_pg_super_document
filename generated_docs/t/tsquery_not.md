# tsquery_not

## Location
src/backend/utils/adt/tsquery_op.c: 159 - 188

## Overview
Creates a negation query that matches documents where the specified tsquery does not match, implementing the NOT logical operator for text search queries.

## Definition


## Detailed Description
The  function creates a NOT operation on a tsquery, which matches documents that do not contain the specified search terms or patterns. It constructs a new query tree with a NOT operator as the root node and the input query as its single child. The function handles empty queries by returning them unchanged, since negating an empty query would still be empty.

The implementation creates a new QTNode structure representing the NOT operator, sets up the necessary flags and metadata, and converts the input query into a child node. The resulting query tree is then converted back into a TSQuery format for return.

## Parameters / Member Variables
- : The tsquery to be negated

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSQUERY_COPY
  - [palloc0](../p/palloc0.md)
  - QT2QTN
  - GETQUERY
  - GETOPERAND  
  - [QTN2QT](../Q/QTN2QT.md)
  - QTNFree
  - PG_FREE_IF_COPY
  - PG_RETURN_POINTER
- Data structures used:
  - TSQuery
  - QTNode
  - QueryItem
  - QTN_NEEDFREE flag
  - QI_OPR type
  - OP_NOT operator

## Notes and Other Information
- Returns the original query unchanged if it's empty (size == 0)
- Creates a single-child query tree with NOT as the root operator
- Properly manages memory allocation and cleanup of intermediate structures
- Part of PostgreSQL's full-text search infrastructure
- Implements the  unary NOT operator in tsquery syntax
- The resulting query will match documents that do not satisfy the original query condition