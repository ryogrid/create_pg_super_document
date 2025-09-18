# Archive

## Location
src/bin/pg_dump/pg_backup.h: 216 - 248

## Overview
A central structure that represents a PostgreSQL dump/restore archive, containing all state information, options, and metadata needed for dump and restore operations.

## Definition
```c
typedef struct Archive
{
    DumpOptions *dopt;           /* options, if dumping */
    RestoreOptions *ropt;        /* options, if restoring */
    
    int         verbose;
    char       *remoteVersionStr; /* server's version string */
    int         remoteVersion;   /* same in numeric form */
    bool        isStandby;       /* is server a standby node */
    
    int         minRemoteVersion; /* allowable range */
    int         maxRemoteVersion;
    
    int         numWorkers;      /* number of parallel processes */
    char       *sync_snapshot_id; /* sync snapshot id for parallel operation */
    
    /* info needed for string escaping */
    int         encoding;        /* libpq code for client_encoding */
    bool        std_strings;     /* standard_conforming_strings */
    
    /* other important stuff */
    char       *searchpath;      /* search_path to set during restore */
    char       *use_role;        /* Issue SET ROLE to this */
    
    /* error handling */
    bool        exit_on_error;   /* whether to exit on SQL errors... */
    int         n_errors;        /* number of errors (if no die) */
    
    /* prepared-query status */
    bool       *is_prepared;     /* indexed by enum _dumpPreparedQueries */
    
    /* The rest is private */
} Archive;
```

## Detailed Description
The Archive structure serves as the core abstraction for PostgreSQL dump and restore operations. It encapsulates all necessary state information, configuration options, and metadata required throughout the entire lifecycle of a dump or restore operation. The structure maintains compatibility information between different PostgreSQL versions, handles parallel processing coordination, manages error states, and provides the foundation for both dumping and restoring database content.

## Parameters / Member Variables
- `dopt`: Pointer to DumpOptions structure when performing dump operations
- `ropt`: Pointer to RestoreOptions structure when performing restore operations
- `verbose`: Verbosity level for output messages
- `remoteVersionStr`: PostgreSQL server version as a string
- `remoteVersion`: PostgreSQL server version in numeric format
- `isStandby`: Flag indicating if the server is a standby/replica node
- `minRemoteVersion`: Minimum compatible PostgreSQL server version
- `maxRemoteVersion`: Maximum compatible PostgreSQL server version
- `numWorkers`: Number of parallel worker processes for the operation
- `sync_snapshot_id`: Snapshot ID used for coordinating parallel operations
- `encoding`: Client encoding identifier (libpq format)
- `std_strings`: Flag for standard_conforming_strings PostgreSQL setting
- `searchpath`: Database search_path to be set during restore operations
- `use_role`: Role name for SET ROLE commands during operations
- `exit_on_error`: Flag controlling whether to exit immediately on SQL errors
- `n_errors`: Counter tracking the number of errors encountered
- `is_prepared`: Array tracking prepared query status indexed by query type enum
- Additional private members not exposed in the public interface

## Dependencies
- Functions called/Symbols referenced:
  - DumpOptions
  - RestoreOptions
- Called from (representative examples):
  - No direct references found in current analysis

## Notes and Other Information
- This structure is defined in src/bin/pg_dump/pg_backup.h:216-248
- Serves as the primary interface between dump/restore operations and the underlying archive format
- Contains both dump-specific (dopt) and restore-specific (ropt) options, but only one is used per operation
- Version compatibility fields ensure safe operations across different PostgreSQL versions
- Parallel processing support through numWorkers and sync_snapshot_id enables efficient large-scale operations
- Error handling mechanism allows for both fail-fast and error-tolerant operation modes
- The encoding and std_strings fields are crucial for proper string handling and SQL generation
- Private members (noted in comment) contain implementation-specific details not part of the public API
- Acts as the central coordination point for all archive-related operations in pg_dump/pg_restore utilities