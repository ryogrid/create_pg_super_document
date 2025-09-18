# A_ArrayExpr

## Location
src/include/nodes/parsenodes.h: 489 - 494

## Overview
A_ArrayExpr represents ARRAY[] construct expressions in PostgreSQL parse trees, used to create array literals from a list of element expressions.

## Definition
```c
typedef struct A_ArrayExpr
{
    NodeTag     type;
    List       *elements;       /* array element expressions */
    ParseLoc    location;       /* token location, or -1 if unknown */
} A_ArrayExpr;
```

## Detailed Description
A_ArrayExpr is a parse tree node that represents the ARRAY[] constructor syntax in SQL. This node encapsulates array literal expressions where users specify array elements explicitly using the ARRAY[elem1, elem2, ...] syntax. The elements list contains expression nodes representing each individual array element, which can be any valid PostgreSQL expression. The location field tracks the position of this construct in the original query text for error reporting and debugging purposes.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an A_ArrayExpr node
- `elements`: List of expression nodes representing the individual array elements
- `location`: ParseLoc indicating the position in the source query, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (inherited node type system)
  - List (PostgreSQL's list data structure)
  - ParseLoc (parse location tracking type)
- Called from (representative examples):
  - transformExprRecurse (src/backend/parser/parse_expr.c:167)
  - transformArrayExpr (src/backend/parser/parse_expr.c:2015, 2041, 2044)
  - transformTypeCast (src/backend/parser/parse_expr.c:2712, 2730)
  - exprLocation (src/backend/nodes/nodeFuncs.c:1680)
  - raw_expression_tree_walker_impl (src/backend/nodes/nodeFuncs.c:4401)

## Notes and Other Information
- Essential for PostgreSQL's array data type support
- Handles explicit array construction with ARRAY[] syntax
- Elements can be any valid expressions, including nested arrays for multidimensional arrays
- Location tracking helps provide accurate error messages during query parsing
- Part of PostgreSQL's expression transformation infrastructure
- Used in type casting operations when arrays are involved