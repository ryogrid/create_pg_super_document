# transformCheckConstraints

## Location
[src/backend/parser/parse_utilcmd.c:2697-2725](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L2697-L2725)

## Overview
Handles CHECK constraints during table creation and alteration, with special logic for skipping validation on new tables while marking constraints as valid.

## Definition

```c
static void
transformCheckConstraints(CreateStmtContext *cxt, bool skipValidation)
```
## Detailed Description
The  function processes CHECK constraints in both CREATE TABLE and ALTER TABLE scenarios. The function implements an optimization for new table creation where CHECK constraint validation can be safely skipped since there are no existing rows to validate against. When validation is skipped, the constraints are automatically marked as valid, overriding any user-supplied NOT VALID flags.

For ALTER TABLE operations, the function currently performs no transformations, but maintains the same calling convention as other constraint transformation functions for consistency. The skipValidation parameter controls whether the optimization can be applied - typically true for CREATE TABLE of regular tables, false for foreign tables and ALTER TABLE operations.

This approach improves performance during table creation by avoiding unnecessary constraint validation while ensuring constraint metadata is correctly established.

## Parameters / Member Variables
- : Pointer to CreateStmtContext containing the CHECK constraints list and table creation context
- : Boolean flag indicating whether constraint validation can be safely skipped (typically true for new table creation)

## Dependencies
- Functions called/Symbols referenced:
  - CreateStmtContext, Constraint (data structures)
  - No major function calls - primarily manipulates constraint flags
- Called from (representative examples):
  - [transformCreateStmt](transformCreateStmt.md) (during CREATE TABLE processing)
  - [transformAlterTableStmt](transformAlterTableStmt.md) (during ALTER TABLE processing)

## Notes and Other Information
- This is a static function in parse_utilcmd.c, part of the constraint transformation infrastructure
- The function is designed to maintain consistency with other constraint transformation functions
- Currently has minimal functionality for ALTER TABLE operations but preserves the interface for future enhancements
- The skipValidation optimization only applies to regular tables, not foreign tables
- When skipValidation is true, it overrides user-supplied NOT VALID specifications
- Part of PostgreSQL's constraint processing pipeline that handles all constraint types uniformly