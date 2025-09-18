# NextValueExpr

## Location
[src/include/nodes/primnodes.h:2109-2114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L2109-L2114)

## Overview
A node representing the generation of the next value from a sequence, equivalent to nextval() but without permission checks, primarily used for identity columns.

## Definition


## Detailed Description
NextValueExpr provides the same functionality as calling the nextval() function on a sequence but bypasses permission checks. This specialized behavior makes it particularly suitable for identity columns, where the sequence is treated as an implicit dependency without requiring separate permissions. The expression node is used internally by PostgreSQL to automatically generate sequential values for identity columns during INSERT operations.

Unlike regular nextval() function calls, NextValueExpr nodes are created and managed automatically by the system when identity columns need values. The lack of permission checking reflects the fact that if a user has INSERT permission on a table with identity columns, they implicitly have the right to advance the associated sequences.

## Parameters / Member Variables
- : Base expression node structure (inherited from Expr)
- : OID of the sequence from which to get the next value
- : OID of the data type that the sequence produces

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - [find_expr_references_walker](../f/find_expr_references_walker.md) (src/backend/catalog/dependency.c:2078)
  - [ATExecAddColumn](../A/ATExecAddColumn.md) (src/backend/commands/tablecmds.c:7270)
  - [ExecInitExprRec](../E/ExecInitExprRec.md) (src/backend/executor/execExpr.c:2578)
  - [build_column_default](../b/build_column_default.md) (src/backend/rewrite/rewriteHandler.c:1229)

## Notes and Other Information
- Primarily used for identity columns where sequences are implicit dependencies
- Bypasses the normal permission checks that apply to explicit nextval() calls
- Created automatically by the system during identity column processing
- The typeId field ensures type safety when the sequence value is used
- Processed during expression initialization and execution phases
- Part of PostgreSQL's identity column implementation, providing SQL standard compliance
- The sequence referenced by seqid must exist and be properly configured for the expected data type
- Used in dependency tracking to maintain proper relationships between tables and their identity sequences