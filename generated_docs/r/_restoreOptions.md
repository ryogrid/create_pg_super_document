# _restoreOptions

## Location
[src/bin/pg_dump/pg_backup.h:94-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup.h#L94-L161)

## Overview
A comprehensive structure that contains all configuration options and parameters needed for the pg_restore utility to control database restoration behavior.

## Definition
```c
typedef struct _restoreOptions
{
    int         createDB;        /* Issue commands to create the database */
    int         noOwner;         /* Don't try to match original object owner */
    int         noTableAm;       /* Don't issue table-AM-related commands */
    int         noTablespace;    /* Don't issue tablespace-related commands */
    int         disable_triggers; /* disable triggers during data-only restore */
    int         use_setsessauth; /* Use SET SESSION AUTHORIZATION commands instead of OWNER TO */
    char       *superuser;       /* Username to use as superuser */
    char       *use_role;        /* Issue SET ROLE to this */
    int         dropSchema;
    int         disable_dollar_quoting;
    int         dump_inserts;    /* 0 = COPY, otherwise rows per INSERT */
    int         column_inserts;
    int         if_exists;
    int         no_comments;     /* Skip comments */
    int         no_publications; /* Skip publication entries */
    int         no_security_labels; /* Skip security label entries */
    int         no_subscriptions; /* Skip subscription entries */
    int         strict_names;
    
    const char *filename;
    int         dataOnly;
    int         schemaOnly;
    int         dumpSections;
    int         verbose;
    int         aclsSkip;
    const char *lockWaitTimeout;
    int         include_everything;
    
    int         tocSummary;
    char       *tocFile;
    int         format;
    char       *formatName;
    
    int         selTypes;
    int         selIndex;
    int         selFunction;
    int         selTrigger;
    int         selTable;
    SimpleStringList indexNames;
    SimpleStringList functionNames;
    SimpleStringList schemaNames;
    SimpleStringList schemaExcludeNames;
    SimpleStringList triggerNames;
    SimpleStringList tableNames;
    
    int         useDB;
    ConnParams  cparams;         /* parameters to use if useDB */
    
    int         noDataForFailedTables;
    int         exit_on_error;
    pg_compress_specification compression_spec; /* Specification for compression */
    int         suppressDumpWarnings; /* Suppress output of WARNING entries to stderr */
    
    bool        single_txn;      /* restore all TOCs in one transaction */
    int         txn_size;        /* restore this many TOCs per txn, if > 0 */
    
    bool       *idWanted;        /* array showing which dump IDs to emit */
    int         enable_row_security;
    int         sequence_data;   /* dump sequence data even in schema-only mode */
    int         binary_upgrade;
    
    char       *restrict_key;
}
```

## Detailed Description
The _restoreOptions structure serves as the central configuration hub for pg_restore operations. It encompasses all user-configurable settings that control how database objects are restored, including object selection, data handling, security settings, and transaction management. This structure provides fine-grained control over the restoration process, allowing users to customize behavior for different restoration scenarios.

## Parameters / Member Variables
- `createDB`: Flag to issue database creation commands
- `noOwner`: Skip object ownership restoration
- `noTableAm`: Skip table access method related commands
- `noTablespace`: Skip tablespace related commands
- `disable_triggers`: Disable triggers during data-only restore
- `use_setsessauth`: Use SET SESSION AUTHORIZATION instead of OWNER TO
- `superuser`: Username to use for superuser operations
- `use_role`: Role name for SET ROLE commands
- `dropSchema`: Drop schema before restore
- `disable_dollar_quoting`: Control dollar quoting usage
- `dump_inserts`: Use INSERT statements instead of COPY (0 = COPY, >0 = rows per INSERT)
- `column_inserts`: Use column names in INSERT statements
- `if_exists`: Use IF EXISTS in DROP commands
- `no_comments`: Skip comment restoration
- `no_publications`: Skip publication entries
- `no_security_labels`: Skip security label entries
- `no_subscriptions`: Skip subscription entries
- `strict_names`: Enforce strict name matching
- `filename`: Input filename for restore
- `dataOnly`: Restore only data, not schema
- `schemaOnly`: Restore only schema, not data
- `dumpSections`: Bitmask of sections to restore
- `verbose`: Verbose output mode
- `aclsSkip`: Skip ACL restoration
- `lockWaitTimeout`: Lock wait timeout value
- `include_everything`: Include all objects by default
- `tocSummary`: Display table of contents summary
- `tocFile`: Table of contents filename
- `format`: Archive format specification
- `formatName`: Human-readable format name
- `selTypes`: Selection flag for types
- `selIndex`: Selection flag for indexes
- `selFunction`: Selection flag for functions
- `selTrigger`: Selection flag for triggers
- `selTable`: Selection flag for tables
- `indexNames`: List of specific indexes to restore
- `functionNames`: List of specific functions to restore
- `schemaNames`: List of specific schemas to restore
- `schemaExcludeNames`: List of schemas to exclude
- `triggerNames`: List of specific triggers to restore
- `tableNames`: List of specific tables to restore
- `useDB`: Flag to use database connection
- `cparams`: Connection parameters when useDB is enabled
- `noDataForFailedTables`: Skip data for tables that fail to create
- `exit_on_error`: Exit immediately on SQL errors
- `compression_spec`: Compression specification settings
- `suppressDumpWarnings`: Suppress WARNING output to stderr
- `single_txn`: Restore all objects in single transaction
- `txn_size`: Number of objects to restore per transaction
- `idWanted`: Array indicating which dump IDs to process
- `enable_row_security`: Enable row-level security during restore
- `sequence_data`: Include sequence data in schema-only mode
- `binary_upgrade`: Enable binary upgrade mode
- `restrict_key`: Key for access restrictions

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleStringList](../S/SimpleStringList.md)
  - [ConnParams](../C/ConnParams.md)
  - [pg_compress_specification](../p/pg_compress_specification.md)
- Called from (representative examples):
  - No direct references found in current analysis

## Notes and Other Information
- This structure is defined in src/bin/pg_dump/pg_backup.h:94-161
- Provides comprehensive control over all aspects of database restoration
- Works in conjunction with Archive structure for complete restore operations
- Many boolean flags allow fine-tuned control over what gets restored and how
- The SimpleStringList members allow selective restoration of specific database objects
- Transaction control options (single_txn, txn_size) provide flexibility for large restore operations