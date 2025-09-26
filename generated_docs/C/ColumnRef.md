# ColumnRef

## Location
[src/include/nodes/parsenodes.h:291-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L291-L296)

## Overview
ColumnRef is a parse tree node that represents references to columns or entire tuples in SQL expressions, supporting qualified column names and wildcard selections.

## Definition
```c
typedef struct ColumnRef
{
    NodeTag     type;
    List       *fields;         /* field names (String nodes) or A_Star */
    ParseLoc    location;       /* token location, or -1 if unknown */
} ColumnRef;
```

## Detailed Description
ColumnRef represents column references in SQL queries, from simple single column names to complex qualified references like "schema.table.column". The structure can also represent wildcard selections using A_Star nodes. The fields list must be nonempty and can contain a mix of String nodes (representing actual names) and A_Star nodes (representing "*" wildcards).

The grammar enforces that A_Star nodes can only appear as the last element in the fields list. For composite columns and container subscripting, additional A_Indirection nodes are placed above the ColumnRef, but initial field selection from table names is represented directly within the ColumnRef for simplicity.

This node type is fundamental to SQL expression parsing and is transformed during semantic analysis to resolve column references to their actual table sources and data types.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a ColumnRef node
- `fields`: List containing String nodes for field/column names and A_Star nodes for wildcards
- `location`: Source location of the column reference in the original SQL text

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc
- Called from (representative examples):
  - transformColumnRef
  - [ExpandColumnRefStar](../E/ExpandColumnRefStar.md)
  - [transformExprRecurse](../t/transformExprRecurse.md)
  - [findTargetlistEntrySQL92](../f/findTargetlistEntrySQL92.md)
  - [transformTargetList](../t/transformTargetList.md)
  - [transformExpressionList](../t/transformExpressionList.md)
  - [FigureColnameInternal](../F/FigureColnameInternal.md)
  - [transformPLAssignStmt](../t/transformPLAssignStmt.md)

## Notes and Other Information
- The fields list represents the qualification hierarchy: ["table", "column"] for table.column
- [A_Star](../A/A_Star.md) nodes are used for SELECT * and table.* expressions
- [ColumnRef](ColumnRef.md) nodes are created by the parser and resolved during semantic analysis
- [Complex](Complex.md) column references like array subscripts use additional A_Indirection nodes above ColumnRef
- Essential for implementing SQL's column reference semantics and name resolution
- Used extensively in target lists, WHERE clauses, and other expression contexts
- The parser ensures proper grammar constraints on wildcard placement
- Location information enables accurate error reporting during semantic analysis