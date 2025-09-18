# bbsink_server_new

## Location
src/backend/backup/basebackup_server.c: 60 - 133

## Overview
Creates a new server-side basebackup sink that stores backup archives directly on the PostgreSQL server filesystem.

## Definition


## Detailed Description
This function creates and initializes a new 'server' bbsink instance for storing basebackup archives on the server filesystem. It performs comprehensive security and permission checks to ensure only authorized users can create server-side backups. The function validates the target directory, creates it if necessary, and enforces strict path requirements to prevent accidental backups to sensitive locations.

The function implements a security model requiring explicit pg_write_server_files role privileges, beyond standard replication permissions. It also enforces absolute path requirements to prevent accidentally storing backups within the data directory being backed up.

## Parameters / Member Variables
- : Pointer to the next bbsink in the chain for chaining multiple backup destinations
- : Absolute path to the directory where backup files will be stored on the server

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [StartTransactionCommand](../S/StartTransactionCommand.md)/CommitTransactionCommand  
  - has_privs_of_role
  - [GetUserId](../G/GetUserId.md)
  - is_absolute_path
  - pg_check_dir
  - MakePGDirectory
  - ereport
  - bbsink_server_ops
- Called from (representative examples):
  - [server_get_sink](../s/server_get_sink.md) (in basebackup_target.c:205)

## Notes and Other Information
- Requires ROLE_PG_WRITE_SERVER_FILES privileges, not just replication permissions
- Enforces absolute paths only to prevent accidental backup to data directory
- Creates target directory with proper PostgreSQL permissions if it doesn't exist
- Validates directory is empty before proceeding with backup
- Returns a bbsink pointer that can be chained with other backup destinations
- Part of PostgreSQL's basebackup infrastructure for server-side backup storage