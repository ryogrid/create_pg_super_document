# vacuumingOptions

## Location
[src/bin/scripts/vacuumdb.c:30-50](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/vacuumdb.c#L30-L50)

## Overview
A structure that encapsulates all user-controllable options for vacuum and analyze operations in the vacuumdb command-line utility.

## Definition
```c
typedef struct vacuumingOptions
{
    bool        analyze_only;
    bool        verbose;
    bool        and_analyze;
    bool        full;
    bool        freeze;
    bool        disable_page_skipping;
    bool        skip_locked;
    int         min_xid_age;
    int         min_mxid_age;
    int         parallel_workers;    /* >= 0 indicates user specified the
                                     * parallel degree, otherwise -1 */
    bool        no_index_cleanup;
    bool        force_index_cleanup;
    bool        do_truncate;
    bool        process_main;
    bool        process_toast;
    bool        skip_database_stats;
    char       *buffer_usage_limit;
} vacuumingOptions;
```

## Detailed Description
The `vacuumingOptions` structure serves as a centralized configuration container for the vacuumdb client utility in PostgreSQL. It consolidates all the command-line options that users can specify to control vacuum and analyze operations behavior. This structure is passed between various functions in the vacuumdb.c file to maintain consistency in how vacuum commands are constructed and executed across different database connections and operations.

The structure supports both simple boolean flags and more complex options like parallel worker configuration and buffer usage limits. It enables the vacuumdb utility to generate appropriate VACUUM and ANALYZE SQL commands based on the PostgreSQL server version and user preferences, ensuring compatibility across different PostgreSQL versions.

## Parameters / Member Variables
- `analyze_only`: When true, only runs ANALYZE command instead of VACUUM
- `verbose`: Enables verbose output during vacuum/analyze operations
- `and_analyze`: Runs ANALYZE after VACUUM when true
- `full`: Performs a full vacuum (VACUUM FULL) which reclaims more space but takes longer
- `freeze`: Performs aggressive tuple freezing to prevent transaction ID wraparound
- `disable_page_skipping`: Disables the visibility map optimization that skips pages
- `skip_locked`: Skips relations that cannot be immediately locked
- `min_xid_age`: Minimum transaction ID age threshold for vacuum operations
- `min_mxid_age`: Minimum multixact ID age threshold for vacuum operations  
- `parallel_workers`: Number of parallel workers to use (-1 means not user-specified)
- `no_index_cleanup`: Disables index cleanup phase during vacuum
- `force_index_cleanup`: Forces index cleanup even when it might be skipped
- `do_truncate`: Enables table truncation to return disk space to the OS
- `process_main`: Processes the main fork of relations
- `process_toast`: Processes TOAST tables associated with relations
- `skip_database_stats`: Skips updating database-wide statistics
- `buffer_usage_limit`: String specifying memory buffer usage limit for operations

## Dependencies
- Functions called/Symbols referenced:
  - do_truncate (member reference)
- Called from (representative examples):
  - VacObjFilter (function parameters in src/bin/scripts/vacuumdb.c:66, 73, 80)
  - main (variable declaration in src/bin/scripts/vacuumdb.c:146)
  - vacuum_one_database (parameter in src/bin/scripts/vacuumdb.c:476)
  - vacuum_all_databases (parameter in src/bin/scripts/vacuumdb.c:910)
  - prepare_vacuum_command (parameter in src/bin/scripts/vacuumdb.c:977)

## Notes and Other Information
- The structure is typically initialized with memset to zero out all fields, then specific defaults are set (parallel_workers = -1, do_truncate = true, process_main = true)
- The parallel_workers field uses -1 as a sentinel value to indicate that the user did not specify a parallel degree
- Buffer usage limit functionality requires PostgreSQL version 16.0 or higher
- Skip locked functionality requires PostgreSQL version 12.0 or higher  
- The parenthesized grammar for ANALYZE options is supported from PostgreSQL version 11.0
- This structure is specific to the client-side vacuumdb utility and is not used by the PostgreSQL server itself
- The structure design allows for easy extension when new vacuum/analyze options are added to PostgreSQL