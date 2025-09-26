# ATPrepAddColumn

## Location
[src/backend/commands/tablecmds.c:6988-7011](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L6988-L7011)

## Overview
Prepares an ADD COLUMN operation for ALTER TABLE by performing validation and handling special cases for typed tables and composite types.

## Definition
```c
static void ATPrepAddColumn(List **wqueue, Relation rel, bool recurse, bool recursing, 
                           bool is_view, AlterTableCmd *cmd, LOCKMODE lockmode, 
                           AlterTableUtilityContext *context)
```

## Detailed Description
This function serves as the preparation phase for ALTER TABLE ADD COLUMN operations. Unlike other ALTER TABLE operations that can use the standard recursion mechanism, ADD COLUMN requires runtime decisions about whether to recurse based on whether a column is actually added or merged with existing columns. This complexity arises from multiple inheritance scenarios that cannot be resolved in a static pre-pass.

The function performs several key validations and preparations:
1. Prevents adding columns to typed tables (tables created with OF clause) unless in a recursive context
2. For composite types, initiates typed table recursion to propagate the change to dependent typed tables
3. Sets the recursion flag for regular tables when recursion is requested and the relation is not a view

The function assumes that constraints like CHECK, NOT NULL, and FOREIGN KEY have already been separated into independent AlterTableCmd entries by the parser.

## Parameters / Member Variables
- `wqueue`: Pointer to the work queue list for ALTER TABLE operations
- `rel`: The relation being altered
- `recurse`: Whether to apply the change to child tables
- `recursing`: Whether this call is part of a recursive operation
- `is_view`: Whether the relation is a view
- `cmd`: The ALTER TABLE command being processed
- `lockmode`: The lock mode to use for operations
- `context`: Context information for the ALTER TABLE utility

## Dependencies
- Functions called/Symbols referenced:
  - ereport
  - [ATTypedTableRecursion](ATTypedTableRecursion.md)
  - [AlterTableCmd](AlterTableCmd.md)
  - [AlterTableUtilityContext](AlterTableUtilityContext.md)
  - RELKIND_COMPOSITE_TYPE
- Called from (representative examples):
  - child_dependency_type
  - [ATPrepCmd](ATPrepCmd.md)

## Notes and Other Information
- The function is static, indicating internal use within tablecmds.c
- ADD COLUMN on typed tables is prohibited because their structure must match the underlying composite type
- For composite types, the function ensures that dependent typed tables are updated accordingly
- The recursion handling is more complex than other ALTER TABLE operations due to inheritance merge scenarios
- Constraints are handled separately from the column addition itself in the ALTER TABLE processing pipeline