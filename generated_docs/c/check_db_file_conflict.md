# check_db_file_conflict

## Location
[src/backend/commands/dbcommands.c:3054-3096](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L3054-L3096)

## Overview
Checks whether a proposed database OID would conflict with existing filesystem objects in any tablespace, preventing accidental overwriting during database creation.

## Definition

```c
struct stat st;
```
## Detailed Description
This function serves as a safety mechanism during database creation to prevent filesystem conflicts. Before PostgreSQL commits to using a specific OID for a new database, this function scans all tablespaces to verify that no directory or file with the same name (the database OID) already exists.

The function implements the same collision-avoidance strategy used by  for table relfilenumber values. Without this check, database creation could succeed initially but then fail during cleanup operations when  attempts to remove what it assumes are database-specific directories, potentially destroying unrelated existing files.

The function performs a comprehensive scan of all tablespaces (except the global tablespace) and constructs the potential database path for each one. If any path already exists in the filesystem, it returns  to indicate a conflict, allowing the database creation logic to try a different OID.

## Parameters / Member Variables
- : The proposed OID for the new database to check for conflicts

## Dependencies
- Functions called/Symbols referenced:
  -  - Open the pg_tablespace system catalog
  -  - Begin scanning the tablespace catalog
  -  - Get next tuple from table scan
  -  - Construct path to potential database directory
  -  - Check if filesystem object exists at the path
  -  - End table scan
  -  - Close table
  -  - Free allocated memory
- Types referenced:
  -  - Structure for tablespace catalog entries
- Called from:
  -  - Called twice during database creation to verify OID uniqueness

## Notes and Other Information
- This is a static (internal) function, not exposed in the public API
- Returns  if any conflict is found,  if the proposed OID is safe to use
- Skips the global tablespace (GLOBALTABLESPACE_OID) since it's shared and managed differently
- Uses  when scanning the tablespace catalog for safe concurrent access
- Performs early termination - stops scanning immediately upon finding the first conflict
- Critical for preventing data loss by avoiding overwrites of existing filesystem objects
- Part of PostgreSQL's robust collision avoidance system for filesystem resources
- The function is defined in 
- Works in conjunction with OID generation logic to ensure unique database identifiers
- Memory management includes proper cleanup with  calls in all code paths