# remove_dbtablespaces

## Location
[src/backend/commands/dbcommands.c:2964-3053](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L2964-L3053)

## Overview
Removes database directories from all tablespaces when a database is being dropped, ensuring complete cleanup of filesystem resources.

## Definition


## Detailed Description
This internal function systematically removes database-specific directories from all tablespaces in the PostgreSQL cluster. When a database is dropped, its data files exist in multiple tablespace directories, and this function ensures complete cleanup by iterating through every tablespace and removing the database's directory (identified by ) from each one.

The function performs a comprehensive tablespace scan, constructs the database path for each tablespace, verifies the directory exists, and removes it along with all contents. It also generates a WAL (Write-Ahead Log) record to ensure the filesystem changes are properly logged for crash recovery and replication purposes.

The function handles errors gracefully - if a directory doesn't exist or can't be removed completely, it continues processing other tablespaces and issues warnings rather than failing the entire operation.

## Parameters / Member Variables
- : The OID of the database whose tablespace directories should be removed

## Dependencies
- Functions called/Symbols referenced:
  -  - Open the pg_tablespace system catalog
  -  - Begin scanning the tablespace catalog
  -  - Get next tuple from table scan
  -  - Construct path to database directory in tablespace
  -  - Check if directory exists and get file status
  -  - Verify path is a directory
  -  - Recursively remove directory and contents
  -  - Append OID to list
  -  - Begin WAL record construction
  -  - Register data for WAL record
  -  - Insert WAL record
  -  - End table scan
  -  - Close table
  -  - Free list memory
  -  - Free memory
  -  - Allocate memory
- Types referenced:
  -  - Structure for tablespace catalog entries
  -  - WAL record structure for database drop
- Called from:
  -  - Cleanup during failed database creation
  -  - Normal database drop operation

## Notes and Other Information
- This is a static (internal) function, not exposed in the public API
- Skips the global tablespace (GLOBALTABLESPACE_OID) as it's shared across all databases
- Issues warnings if some files cannot be removed but continues processing
- Generates WAL records with  type and  flag for proper crash recovery
- Uses  when scanning the tablespace catalog to avoid conflicts
- Memory management is handled carefully with  calls to prevent leaks
- The function is defined in 
- Critical for maintaining filesystem cleanliness and preventing orphaned database directories