# TablespaceCreateDbspace

## Location
[src/backend/commands/tablespace.c:112-207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L112-L207)

## Overview
Creates database-specific subdirectories within tablespaces to isolate each database's objects into its own namespace, handling both normal operation and WAL replay scenarios.

## Definition


## Detailed Description
TablespaceCreateDbspace ensures that each database using a tablespace is isolated into its own namespace by creating a subdirectory named for the database OID. The function handles both normal operations and WAL replay scenarios, with special logic to cope with missing directories during recovery.

The function performs atomic directory creation using TablespaceCreateLock to prevent race conditions with concurrent DROP TABLESPACE operations. During WAL replay (isRedo=true), it employs a more permissive approach, creating directory hierarchies as needed to handle cases where tablespaces may have been dropped ahead in the WAL stream.

For the global tablespace (GLOBALTABLESPACE_OID), the function returns early as it doesn't require per-database subdirectories.

## Parameters / Member Variables
- : The OID of the tablespace where the database subdirectory should be created
- : The OID of the database for which to create the subdirectory
- : Boolean flag indicating whether this is being called during WAL replay, which affects error handling behavior

## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabasePath](../G/GetDatabasePath.md): Constructs the full path for the database directory
  - S_ISDIR: System macro to check if a file is a directory
  - MakePGDirectory: PostgreSQL wrapper for creating directories
  - [pg_mkdir_p](../p/pg_mkdir_p.md): Creates directory hierarchies recursively
- Called from (representative examples):
  - [mdcreate](../m/mdcreate.md): During relation file creation

## Notes and Other Information
- Uses TablespaceCreateLock (LW_EXCLUSIVE) to ensure atomic directory creation
- During WAL replay, employs fallback strategies for missing directory hierarchies
- Global tablespace is exempt from per-database subdirectory creation
- Performs double-checked locking pattern to avoid unnecessary work
- Error handling differs between normal operation and WAL replay modes