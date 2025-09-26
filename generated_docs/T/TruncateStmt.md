# TruncateStmt

## Location
src/include/nodes/parsenodes.h: 3240 - 3246

## Overview
TruncateStmt represents a TRUNCATE TABLE statement in the PostgreSQL parser, providing efficient removal of all rows from one or more tables.

## Definition


## Detailed Description
TruncateStmt is a parse tree node that represents the TRUNCATE TABLE SQL statement. TRUNCATE provides a fast way to remove all rows from a table or set of tables without scanning the table data, making it much more efficient than DELETE statements for removing all table contents. The operation also reclaims disk space immediately.

The statement supports truncating multiple tables simultaneously and can handle sequence restart behavior through the restart_seqs flag, which corresponds to the RESTART IDENTITY clause. The behavior field controls how foreign key constraints are handled - RESTRICT mode prevents truncation if foreign key references exist, while CASCADE mode truncates referencing tables as well.

TRUNCATE operations require special privileges and have various restrictions, such as not being allowed on tables with foreign key references (unless CASCADE is used) and not being possible within user-defined functions.

## Parameters / Member Variables
- : NodeTag identifier for this parse tree node type
- : List of RangeVar nodes specifying the tables to be truncated (supports multiple tables in one statement)
- : Boolean flag for RESTART IDENTITY behavior - when true, sequences owned by the table columns are reset to their start values
- : DropBehavior enum controlling foreign key constraint handling (DROP_RESTRICT or DROP_CASCADE)

## Dependencies
- Functions called/Symbols referenced:
  - DropBehavior
- Called from (representative examples):
  - ExecuteTruncate
  - standard_ProcessUtility

## Notes and Other Information
- TRUNCATE is much faster than DELETE because it doesn't scan table rows or generate individual row delete log entries
- The operation immediately reclaims disk space, unlike DELETE which may require VACUUM
- RESTART IDENTITY resets sequences associated with table columns (typically SERIAL columns)
- CASCADE behavior can truncate additional tables beyond those explicitly listed if they reference the target tables
- TRUNCATE operations cannot be performed inside user-defined functions due to transaction control limitations
- The statement acquires an ACCESS EXCLUSIVE lock on target tables, blocking all concurrent access
- Multiple tables can be truncated in a single statement, and the operation is atomic across all specified tables