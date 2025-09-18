# CheckAlterTableIsSafe

## Location
[src/backend/commands/tablecmds.c:4314-4339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L4314-L4339)

## Overview
Verifies that it's safe to perform ALTER TABLE operations on a relation by checking for temp table ownership conflicts and ensuring the table is not actively in use by the current session.

## Definition
```c
static void CheckAlterTableIsSafe(Relation rel)
```

## Detailed Description
This function provides a comprehensive safety check specifically for ALTER TABLE operations by combining two critical verifications:

1. **Temporary Table Ownership Check**: Prevents ALTER TABLE operations on temporary tables that belong to other database sessions. This restriction exists because other sessions' local buffer managers cannot cope with changes to their temporary tables, and there are various optimizations that assume temporary tables are not subject to external interference.

2. **Active Usage Check**: Delegates to CheckTableNotInUse() to verify that the relation is not currently being used by open cursors, active plans, or has pending AFTER trigger events in the current transaction.

This function is specifically tailored for ALTER TABLE operations and provides a more restrictive check than CheckTableNotInUse() alone. While DROP TABLE operations are allowed on orphaned temporary tables (to enable cleanup), ALTER TABLE operations require stricter safety guarantees due to their potential for structural modification.

## Parameters / Member Variables
- `rel`: The relation to be checked for ALTER TABLE safety

## Dependencies
- Functions called/Symbols referenced:
  - RELATION_IS_OTHER_TEMP (macro to check if relation is another session's temp table)
  - [CheckTableNotInUse](CheckTableNotInUse.md) (performs active usage checks with "ALTER TABLE" as the statement name)
  - ereport (reports errors with appropriate error codes)
- Called from (representative examples):
  - [AlterTable](../A/AlterTable.md) (in tablecmds.c:4407, main entry point for ALTER TABLE operations)
  - [ATSimpleRecursion](../A/ATSimpleRecursion.md) (in tablecmds.c:6648, during recursive ALTER TABLE operations)
  - [ATExecAddColumn](../A/ATExecAddColumn.md) (in tablecmds.c:7416, before adding columns)
  - [ATExecDropColumn](../A/ATExecDropColumn.md) (in tablecmds.c:9093, before dropping columns)
  - [ATPrepAlterColumnType](../A/ATPrepAlterColumnType.md) (in tablecmds.c:13017, before changing column types)
  - [ATExecDropConstraint](../A/ATExecDropConstraint.md) (in tablecmds.c:12626, 12703, before dropping constraints)

## Notes and Other Information
- This is a static function within tablecmds.c, indicating it's an internal helper for ALTER TABLE operations
- The function combines two different types of safety checks that have different rationales and error conditions
- Uses ERRCODE_FEATURE_NOT_SUPPORTED for temp table ownership violations, indicating this is a deliberate design limitation
- The distinction from CheckTableNotInUse() is necessary because DROP TABLE has different requirements and must be able to clean up orphaned temp schemas
- Part of PostgreSQL's layered approach to DDL safety, where different operations have different safety requirements
- The temp table check is performed first as a quick rejection before the more expensive active usage checks
- This function is called extensively throughout the ALTER TABLE command processing pipeline to ensure safety at each major step