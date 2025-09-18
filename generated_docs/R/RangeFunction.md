# RangeFunction

## Location
[src/include/nodes/parsenodes.h:637-647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L637-L647)

## Overview
RangeFunction is a parse tree node that represents function calls appearing in FROM clauses, supporting both simple function calls and complex ROWS FROM() constructs with multiple functions, optional column definitions, and ordinality columns.

## Definition


## Detailed Description
RangeFunction nodes handle function calls that serve as table sources in FROM clauses. This includes simple function calls like "SELECT * FROM generate_series(1,10)" and complex ROWS FROM() constructs that can combine multiple functions. The functions list contains two-element sublists: the first element is the untransformed function call tree, and the second is a possibly-empty list of ColumnDef nodes for any columndef list attached to that specific function. The structure supports LATERAL correlation, WITH ORDINALITY for row numbering, and top-level column definitions for functions returning RECORD types.

## Parameters / Member Variables
- : Standard NodeTag identifying this as a RangeFunction node
- : Boolean flag indicating whether the LATERAL keyword was specified, enabling correlation with preceding FROM clause items
- : Boolean flag indicating whether WITH ORDINALITY was specified to add a row number column
- : Boolean flag indicating whether this represents a ROWS FROM() construct versus a simple function call
- : List containing per-function information as two-element sublists (function call tree + optional ColumnDef list)
- : Optional Alias structure for table alias name and column aliases
- : List of ColumnDef nodes describing the result structure for functions returning RECORD type

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (inherited structure member)
  - [List](../L/List.md) (for functions and column definitions)
  - [Alias](../A/Alias.md) (for table and column aliasing)
  - [ColumnDef](../C/ColumnDef.md) (implicitly referenced for column definitions)
- Called from (representative examples):
  - [transformRangeFunction](../t/transformRangeFunction.md) (src/backend/parser/parse_clause.c:465)
  - [transformFromClauseItem](../t/transformFromClauseItem.md) (src/backend/parser/parse_clause.c:1096, 1102)
  - [addRangeTableEntryForFunction](../a/addRangeTableEntryForFunction.md) (src/backend/parser/parse_relation.c:1738)
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md) (src/backend/nodes/nodeFuncs.c:4454)

## Notes and Other Information
- [RangeFunction](RangeFunction.md) supports both simple function-in-FROM and complex ROWS FROM() multi-function constructs
- The functions list structure allows multiple functions to be combined in a single ROWS FROM() call
- WITH ORDINALITY adds an implicit row number column to the function results
- LATERAL functions can reference columns from preceding tables in the FROM clause
- Column definition lists are essential for functions returning RECORD type where the result structure is not predetermined
- Parse analysis validates that column definitions don't appear both per-function and at the top level
- File location: src/include/nodes/parsenodes.h:637-647