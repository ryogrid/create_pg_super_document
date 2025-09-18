# _dumpOptions

## Location
[src/bin/pg_dump/pg_backup.h:164-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup.h#L164-L209)

## Overview
A structure that contains all configuration options and parameters needed for the pg_dump utility to control database dumping behavior and output format.

## Definition
```c
typedef struct _dumpOptions
{
    ConnParams  cparams;
    
    int         binary_upgrade;
    
    /* various user-settable parameters */
    bool        schemaOnly;
    bool        dataOnly;
    int         dumpSections;    /* bitmask of chosen sections */
    bool        aclsSkip;
    const char *lockWaitTimeout;
    int         dump_inserts;    /* 0 = COPY, otherwise rows per INSERT */
    
    /* flags for various command-line long options */
    int         disable_dollar_quoting;
    int         column_inserts;
    int         if_exists;
    int         no_comments;
    int         no_security_labels;
    int         no_publications;
    int         no_subscriptions;
    int         no_toast_compression;
    int         no_unlogged_table_data;
    int         serializable_deferrable;
    int         disable_triggers;
    int         outputNoTableAm;
    int         outputNoTablespaces;
    int         use_setsessauth;
    int         enable_row_security;
    int         load_via_partition_root;
    
    /* default, if no "inclusion" switches appear, is to dump everything */
    bool        include_everything;
    
    int         outputClean;
    int         outputCreateDB;
    bool        outputLOs;
    bool        dontOutputLOs;
    int         outputNoOwner;
    char       *outputSuperuser;
    
    int         sequence_data;   /* dump sequence data even in schema-only mode */
    int         do_nothing;
    
    char       *restrict_key;
}
```

## Detailed Description
The _dumpOptions structure serves as the central configuration container for pg_dump operations. It controls all aspects of the database dumping process, including what objects to dump, how to format the output, and various behavioral options. This structure allows fine-grained control over the dump process, enabling users to customize the output for different use cases such as backup, migration, or selective data extraction.

## Parameters / Member Variables
- `cparams`: Connection parameters for database access
- `binary_upgrade`: Enable binary upgrade mode for version upgrades
- `schemaOnly`: Dump only schema definitions, not data
- `dataOnly`: Dump only data, not schema definitions
- `dumpSections`: Bitmask specifying which sections to include in dump
- `aclsSkip`: Skip access control list (ACL) dumping
- `lockWaitTimeout`: Timeout value for acquiring locks
- `dump_inserts`: Use INSERT statements instead of COPY (0 = COPY, >0 = rows per INSERT)
- `disable_dollar_quoting`: Disable dollar quoting in output
- `column_inserts`: Include column names in INSERT statements
- `if_exists`: Add IF EXISTS clauses to DROP commands
- `no_comments`: Exclude comments from dump output
- `no_security_labels`: Exclude security labels from dump
- `no_publications`: Exclude publication definitions
- `no_subscriptions`: Exclude subscription definitions
- `no_toast_compression`: Exclude TOAST compression settings
- `no_unlogged_table_data`: Exclude data from unlogged tables
- `serializable_deferrable`: Use serializable deferrable transactions
- `disable_triggers`: Add commands to disable triggers during restore
- `outputNoTableAm`: Exclude table access method specifications
- `outputNoTablespaces`: Exclude tablespace assignments
- `use_setsessauth`: Use SET SESSION AUTHORIZATION instead of OWNER TO
- `enable_row_security`: Enable row-level security during dump
- `load_via_partition_root`: Load partitioned table data via root table
- `include_everything`: Include all database objects by default
- `outputClean`: Add DROP commands before CREATE commands
- `outputCreateDB`: Include database creation commands
- `outputLOs`: Include large objects in dump
- `dontOutputLOs`: Explicitly exclude large objects
- `outputNoOwner`: Exclude object ownership commands
- `outputSuperuser`: Username to use for superuser operations in dump
- `sequence_data`: Include sequence data even in schema-only mode
- `do_nothing`: Perform dry run without actual dumping
- `restrict_key`: Access restriction key

## Dependencies
- Functions called/Symbols referenced:
  - [ConnParams](../C/ConnParams.md)
- Called from (representative examples):
  - No direct references found in current analysis

## Notes and Other Information
- This structure is defined in src/bin/pg_dump/pg_backup.h:164-209
- Provides comprehensive control over all aspects of database dumping
- Boolean and integer flags allow precise customization of dump behavior
- Works with ConnParams for database connectivity
- The dump_inserts option provides flexibility between COPY and INSERT statement formats
- Multiple exclusion options (no_comments, no_security_labels, etc.) allow selective dumping
- Output formatting options support different restore scenarios and requirements