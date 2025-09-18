# TSConfigInfo

## Location
src/bin/pg_dump/pg_dump.h: 563 - 564

## Overview
TSConfigInfo is a structure used in pg_dump to represent a PostgreSQL text search configuration, storing metadata needed to dump and restore text search configurations.

## Definition
```c
typedef struct _cfgInfo
{
    DumpableObject dobj;
    const char *rolname;
    Oid         cfgparser;
} TSConfigInfo;
```

## Detailed Description
TSConfigInfo is part of pg_dump's internal representation of PostgreSQL database objects that need to be dumped and restored. It specifically handles text search configurations, which are the main components users interact with in PostgreSQL's full-text search system. A text search configuration defines how documents are parsed and which dictionaries are used to process different types of tokens. The structure stores the parser used by the configuration and the owner role. This information is retrieved from the pg_ts_config system catalog and used to generate CREATE TEXT SEARCH CONFIGURATION statements during database dumps.

## Parameters / Member Variables
- `dobj`: Base DumpableObject containing common metadata (name, namespace, dump ID, object type)
- `rolname`: Name of the role that owns this text search configuration
- `cfgparser`: OID of the text search parser used by this configuration

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - getTSConfigurations (populates TSConfigInfo structures from pg_ts_config catalog)
  - dumpTSConfig (uses TSConfigInfo to generate CREATE TEXT SEARCH CONFIGURATION statements)
  - fmtQualifiedDumpable (formats the configuration name for output)

## Notes and Other Information
- Located in src/bin/pg_dump/pg_dump.h:558-563
- Used exclusively within pg_dump for backing up and restoring text search configurations
- The structure maps directly to columns in the pg_ts_config system catalog
- Text search configurations are the primary interface for full-text search operations
- Each configuration is associated with a specific parser and defines token-to-dictionary mappings
- Part of PostgreSQL's full-text search infrastructure, serving as the top-level object that coordinates parsing and dictionary processing
- The cfgparser field references a text search parser that determines how text is tokenized