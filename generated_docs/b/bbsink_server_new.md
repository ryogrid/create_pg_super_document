# bbsink_server_new

## Location
[src/backend/backup/basebackup_server.c:60-133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_server.c#L60-L133)

## Overview
Creates a new server-side basebackup sink that stores backup archives directly on the PostgreSQL server filesystem.

## Definition

```c
bbsink *
bbsink_server_new(bbsink *next, char *pathname)
```
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
  - [has_privs_of_role](../h/has_privs_of_role.md)
  - [GetUserId](../G/GetUserId.md)
  - is_absolute_path
  - [pg_check_dir](../p/pg_check_dir.md)
  - [MakePGDirectory](../M/MakePGDirectory.md)
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

## Simplified Source

```c
// Simplified version of bbsink_server_new
bbsink *bbsink_server_new(bbsink *next, char *pathname) {
    bbsink_server *sink = palloc0(sizeof(bbsink_server));

    // Initialize bbsink structure
    *((const bbsink_ops **) &sink->base.bbs_ops) = &bbsink_server_ops;
    sink->pathname = pathname;
    sink->base.bbs_next = next;

    // Check role privileges for server file writing
    StartTransactionCommand();
    if (!has_privs_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES)) {
        ereport(ERROR,
               (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to create backup stored on server"),
                errdetail("Only roles with privileges of the \"pg_write_server_files\" role may create a backup stored on the server.")));
    }
    CommitTransactionCommand();

    // Require absolute path to prevent accidental backup to data directory
    if (!is_absolute_path(pathname)) {
        ereport(ERROR,
               (errcode(ERRCODE_INVALID_NAME),
                errmsg("relative path not allowed for backup stored on server")));
    }

    // Check and create directory as needed
    switch (pg_check_dir(pathname)) {
        case 0:
            // Create directory if it doesn't exist
            if (MakePGDirectory(pathname) < 0) {
                ereport(ERROR,
                       (errcode_for_file_access(),
                        errmsg("could not create directory \"%s\": %m", pathname)));
            }
            break;
        case 1:
            // Directory exists and is empty
            break;
        case 2:
        case 3:
        case 4:
            // Directory exists but is not empty
            ereport(ERROR,
                   (errcode(ERRCODE_DUPLICATE_FILE),
                    errmsg("directory \"%s\" exists but is not empty", pathname)));
            break;
        default:
            // Access problem
            ereport(ERROR,
                   (errcode_for_file_access(),
                    errmsg("could not access directory \"%s\": %m", pathname)));
    }

    return &sink->base;
}
```

Key simplifications made:
- Preserved complete security validation and permission checks
- Maintained directory creation and validation logic
- Kept essential error handling for various directory states
- Focused on core bbsink initialization and safety mechanisms