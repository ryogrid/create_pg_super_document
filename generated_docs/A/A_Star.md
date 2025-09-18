# A_Star

## Location
[src/include/nodes/parsenodes.h:445-448](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L445-L448)

## Overview
A_Star represents the '*' wildcard symbol in SQL queries, used to indicate all columns of a table or all elements of a compound field.

## Definition


## Detailed Description
A_Star is a simple node structure that represents the asterisk ('*') symbol in PostgreSQL's parse tree. It serves as a placeholder for "all columns" in SELECT statements or "all fields" when accessing compound data types. This node can appear in various contexts within the parser tree, specifically within ColumnRef.fields, A_Indirection.indirection, and ResTarget.indirection lists. The structure is minimal, containing only the standard NodeTag to identify its type within PostgreSQL's node system.

## Parameters / Member Variables
- : NodeTag identifying this as an A_Star node in PostgreSQL's node hierarchy

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (inherited node type system)
- Called from (representative examples):
  - [transformIndirection](../t/transformIndirection.md) (src/backend/parser/parse_expr.c:457)
  - [transformTargetList](../t/transformTargetList.md) (src/backend/parser/parse_target.c:149, 163)
  - [transformExpressionList](../t/transformExpressionList.md) (src/backend/parser/parse_target.c:239, 252)
  - [sql_fn_post_column_ref](../s/sql_fn_post_column_ref.md) (src/backend/executor/functions.c:317)
  - [ProcessCopyOptions](../P/ProcessCopyOptions.md) (src/backend/commands/copy.c:553, 568, 583)

## Notes and Other Information
- This node type is fundamental to SQL's SELECT * functionality
- Appears in parse trees when the parser encounters '*' in column references
- Part of PostgreSQL's expression and target list processing infrastructure
- Used during query transformation phases to expand wildcard references into explicit column lists