# TSDictInfo

## Location
[src/bin/pg_dump/pg_dump.h:549-550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L549-L550)

## Overview
TSDictInfo is a structure used in pg_dump to represent a PostgreSQL text search dictionary, storing metadata needed to dump and restore text search dictionaries.

## Definition
```c
typedef struct _dictInfo
{
    DumpableObject dobj;
    const char *rolname;
    Oid         dicttemplate;
    char       *dictinitoption;
} TSDictInfo;
```

## Detailed Description
TSDictInfo is part of pg_dump's internal representation of PostgreSQL database objects that need to be dumped and restored. It specifically handles text search dictionaries, which are components of PostgreSQL's full-text search system used for processing tokens during text indexing and searching. The structure stores the template used to create the dictionary, the owner role, and any initialization options. This information is retrieved from the pg_ts_dict system catalog and used to generate CREATE TEXT SEARCH DICTIONARY statements during database dumps.

## Parameters / Member Variables
- `dobj`: Base DumpableObject containing common metadata (name, namespace, dump ID, object type)
- `rolname`: Name of the role that owns this text search dictionary
- `dicttemplate`: OID of the dictionary template that this dictionary is based on
- `dictinitoption`: Initialization options passed to the dictionary template (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [getTSDictionaries](../g/getTSDictionaries.md) (populates TSDictInfo structures from pg_ts_dict catalog)
  - [dumpTSDictionary](../d/dumpTSDictionary.md) (uses TSDictInfo to generate CREATE TEXT SEARCH DICTIONARY statements)
  - fmtQualifiedDumpable (formats the dictionary name for output)

## Notes and Other Information
- Located in src/bin/pg_dump/pg_dump.h:543-549
- Used exclusively within pg_dump for backing up and restoring text search dictionaries
- The structure maps directly to columns in the pg_ts_dict system catalog
- The dictinitoption field can be NULL if no initialization options were specified when the dictionary was created
- Part of PostgreSQL's full-text search infrastructure, allowing customizable token processing for different languages and use cases
- The dicttemplate field references a text search template that defines the dictionary's behavior