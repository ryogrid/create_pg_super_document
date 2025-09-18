# TSTemplateInfo

## Location
[src/bin/pg_dump/pg_dump.h:556-557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L556-L557)

## Overview
TSTemplateInfo is a structure used in pg_dump to represent a PostgreSQL text search template, storing metadata needed to dump and restore text search templates.

## Definition
```c
typedef struct _tmplInfo
{
    DumpableObject dobj;
    Oid         tmplinit;
    Oid         tmpllexize;
} TSTemplateInfo;
```

## Detailed Description
TSTemplateInfo is part of pg_dump's internal representation of PostgreSQL database objects that need to be dumped and restored. It specifically handles text search templates, which are blueprints for creating text search dictionaries in PostgreSQL's full-text search system. The structure stores the OIDs of the two functions that define the template's behavior: an initialization function and a lexize function. This information is retrieved from the pg_ts_template system catalog and used to generate CREATE TEXT SEARCH TEMPLATE statements during database dumps.

## Parameters / Member Variables
- `dobj`: Base DumpableObject containing common metadata (name, namespace, dump ID, object type)
- `tmplinit`: OID of the template's initialization function that prepares the dictionary
- `tmpllexize`: OID of the template's lexize function that processes tokens

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [getTSTemplates](../g/getTSTemplates.md) (populates TSTemplateInfo structures from pg_ts_template catalog)
  - [dumpTSTemplate](../d/dumpTSTemplate.md) (uses TSTemplateInfo to generate CREATE TEXT SEARCH TEMPLATE statements)
  - fmtQualifiedDumpable (formats the template name for output)

## Notes and Other Information
- Located in src/bin/pg_dump/pg_dump.h:551-556
- Used exclusively within pg_dump for backing up and restoring text search templates
- The structure maps directly to columns in the pg_ts_template system catalog
- Text search templates serve as the foundation for creating text search dictionaries
- The tmplinit function is optional and can be NULL, while tmpllexize is required
- Part of PostgreSQL's full-text search infrastructure, providing reusable patterns for dictionary creation
- Templates encapsulate the logic for token processing that can be shared across multiple dictionaries