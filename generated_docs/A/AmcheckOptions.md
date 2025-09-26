# AmcheckOptions

## Location
[src/bin/pg_amcheck/pg_amcheck.c:54-109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L54-L109)

## Overview
AmcheckOptions is a comprehensive configuration structure used in PostgreSQL's pg_amcheck utility to store all command-line options and settings that control the behavior of data integrity checking operations.

## Definition
```c
typedef struct AmcheckOptions
{
    bool        dbpattern;
    bool        alldb;
    bool        echo;
    bool        verbose;
    bool        strict_names;
    bool        show_progress;
    int         jobs;

    /* Whether to install missing extensions, and optionally the name of the
     * schema in which to install the extension's objects. */
    bool        install_missing;
    char       *install_schema;

    /* Objects to check or not to check, as lists of PatternInfo structs. */
    PatternInfoArray include;
    PatternInfoArray exclude;

    /* As an optimization, if any pattern in the exclude list applies to heap
     * tables, or similarly if any such pattern applies to btree indexes, or
     * to schemas, then these will be true, otherwise false.  These should
     * always agree with what you'd conclude by grep'ing through the exclude
     * list. */
    bool        excludetbl;
    bool        excludeidx;
    bool        excludensp;

    /* If any inclusion pattern exists, then we should only be checking
     * matching relations rather than all relations, so this is true iff
     * include is empty. */
    bool        allrel;

    /* heap table checking options */
    bool        no_toast_expansion;
    bool        reconcile_toast;
    bool        on_error_stop;
    int64       startblock;
    int64       endblock;
    const char *skip;

    /* btree index checking options */
    bool        parent_check;
    bool        rootdescend;
    bool        heapallindexed;
    bool        checkunique;

    /* heap and btree hybrid option */
    bool        no_btree_expansion;
} AmcheckOptions;
```

## Detailed Description
The AmcheckOptions structure serves as the central configuration object for pg_amcheck, encapsulating all user-controllable options that influence the integrity checking process. It organizes options into several categories: general control flags (verbose, echo, progress), extension management settings, object selection patterns (include/exclude lists), optimization flags for pattern matching, and specific checking options for heap tables and btree indexes. The structure supports both inclusive and exclusive pattern matching through PatternInfoArray members, and includes optimization flags that track whether certain types of exclusions are active to avoid unnecessary pattern matching operations.

## Parameters / Member Variables
- `dbpattern`: Flag indicating database pattern matching is enabled
- `alldb`: Flag to check all databases
- `echo`: Flag to echo SQL commands being executed
- `verbose`: Flag to enable verbose output
- `strict_names`: Flag to enforce strict name matching
- `show_progress`: Flag to display progress information
- `jobs`: Number of parallel jobs to run
- `install_missing`: Flag to automatically install missing extensions
- `install_schema`: Schema name for installing extension objects
- `include`: Array of patterns for objects to include in checking
- `exclude`: Array of patterns for objects to exclude from checking
- `excludetbl`: Optimization flag indicating heap table exclusions exist
- `excludeidx`: Optimization flag indicating btree index exclusions exist
- `excludensp`: Optimization flag indicating schema exclusions exist
- `allrel`: Flag indicating all relations should be checked (true if include list is empty)
- `no_toast_expansion`: Flag to disable TOAST table expansion for heap checking
- `reconcile_toast`: Flag to enable TOAST table reconciliation
- `on_error_stop`: Flag to stop processing on first error
- `startblock`: Starting block number for range-limited heap checking
- `endblock`: Ending block number for range-limited heap checking
- `skip`: Comma-separated list of check types to skip
- `parent_check`: Flag to enable parent-child relationship checking for btree indexes
- `rootdescend`: Flag to enable root-to-leaf descent checking for btree indexes
- `heapallindexed`: Flag to verify all heap tuples are indexed
- `checkunique`: Flag to verify uniqueness constraints in btree indexes
- `no_btree_expansion`: Flag to disable btree index expansion during heap checking

## Dependencies
- Functions called/Symbols referenced:
  - [PatternInfoArray](../P/PatternInfoArray.md) (for include and exclude members)
  - [skip](../s/skip.md) (string member)
- Called from (representative examples):
  - (Structure definition, not directly called)

## Notes and Other Information
- Defined in src/bin/pg_amcheck/pg_amcheck.c:53-109
- Central configuration structure for the pg_amcheck utility
- Supports both heap table and btree index checking with specialized options for each
- Includes optimization flags (excludetbl, excludeidx, excludensp) to avoid unnecessary pattern matching
- The allrel flag provides a quick check for whether inclusion patterns are active
- [Range](../R/Range.md) checking for heap tables is supported through startblock and endblock members
- Extension management capabilities allow automatic installation of required extensions