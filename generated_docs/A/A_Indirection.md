# A_Indirection

## Location
src/include/nodes/parsenodes.h: 479 - 484

## Overview
A_Indirection represents complex field and array element selection operations from expressions, supporting chained access to nested data structures.

## Definition
```c
typedef struct A_Indirection
{
    NodeTag     type;
    Node       *arg;            /* the thing being selected from */
    List       *indirection;    /* subscripts and/or field names and/or * */
} A_Indirection;
```

## Detailed Description
A_Indirection is a sophisticated parse tree node that handles complex data access patterns in PostgreSQL. It represents operations like field selection, array subscripting, and wildcard expansion from a base expression. The indirection list can contain various node types: A_Indices nodes for array subscripting, String nodes for field selection (where the string value is the field name), and A_Star nodes for selecting all fields of a composite type. For example, the expression (foo).field1[42][7].field2 would be represented as a single A_Indirection node with a 4-element indirection list. The grammar enforces that A_Star nodes can only appear as the last element in the indirection list.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an A_Indirection node
- `arg`: Expression node representing the base object being accessed
- `indirection`: List containing the sequence of access operations (field names, array indices, or wildcard selectors)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (inherited node type system)
  - [Node](../N/Node.md) (base node type for expressions)
  - [List](../L/List.md) (PostgreSQL's list data structure)
- Called from (representative examples):
  - [transformExprRecurse](../t/transformExprRecurse.md) (src/backend/parser/parse_expr.c:163)
  - [transformIndirection](../t/transformIndirection.md) (src/backend/parser/parse_expr.c:438)
  - [transformTargetList](../t/transformTargetList.md) (src/backend/parser/parse_target.c:159, 161)
  - [transformExpressionList](../t/transformExpressionList.md) (src/backend/parser/parse_target.c:248, 250)
  - [ExpandIndirectionStar](../E/ExpandIndirectionStar.md) (src/backend/parser/parse_target.c:1345)
  - [FigureColnameInternal](../F/FigureColnameInternal.md) (src/backend/parser/parse_target.c:1774)

## Notes and Other Information
- Central to PostgreSQL's support for complex data types like arrays, composites, and JSON
- Enables chained access operations in a single parse tree node
- Used extensively in query transformation and target list processing
- Grammar rules ensure A_Star appears only at the end of indirection chains
- Critical for expanding wildcard selections into explicit column references
- Part of PostgreSQL's expression evaluation infrastructure for nested data access