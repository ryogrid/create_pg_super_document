# DoCopy

## Location
[src/backend/commands/copy.c:62-328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copy.c#L62-L328)

## Overview
DoCopy executes the SQL COPY statement, handling both copying data from files/programs/stdin into tables (COPY FROM) and copying table data or query results to files/programs/stdout (COPY TO).

## Definition


## Detailed Description
DoCopy is the main entry point for executing COPY statements in PostgreSQL. It performs comprehensive permission checking, handles both table-based and query-based COPY operations, and manages row-level security (RLS) requirements. For COPY FROM operations, it transfers data from external sources into database tables. For COPY TO operations, it exports table data or query results to external destinations. The function handles various security restrictions including role-based permissions for file and program access, and automatically converts table-based COPY TO operations to query-based operations when row-level security is enabled.

## Parameters / Member Variables
- : ParseState containing query parsing context and namespace information
- : CopyStmt structure containing the parsed COPY statement details including source/destination, options, and column lists  
- : Character position where the COPY statement starts in the original query string
- : Length of the COPY statement in characters
- : Output parameter returning the number of rows processed during the COPY operation

## Dependencies
- Functions called/Symbols referenced:
  - has_privs_of_role
  - table_openrv
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md)
  - [transformExpr](../t/transformExpr.md)
  - [CopyGetAttnums](../C/CopyGetAttnums.md)
  - [ExecCheckPermissions](../E/ExecCheckPermissions.md)
  - [check_enable_rls](../c/check_enable_rls.md)
  - [BeginCopyFrom](../B/BeginCopyFrom.md)/CopyFrom/EndCopyFrom
  - [BeginCopyTo](../B/BeginCopyTo.md)/DoCopyTo/EndCopyTo
  - [PreventCommandIfReadOnly](../P/PreventCommandIfReadOnly.md)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Enforces strict permission checking for file and program access through role-based security
- Automatically handles row-level security by converting table COPY TO operations to SELECT-based operations
- Supports WHERE clauses for filtering data during COPY operations
- Manages proper locking (RowExclusiveLock for COPY FROM, AccessShareLock for COPY TO)
- Prevents COPY FROM operations when row-level security is enabled, requiring INSERT statements instead
- Handles both pipe-based operations (stdin/stdout) and file-based operations with appropriate security checks