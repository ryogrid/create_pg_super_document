# _PublicationSchemaInfo

## Location
src/bin/pg_dump/pg_dump.h: 661 - 665

## Overview
The `_PublicationSchemaInfo` struct represents publication schema mapping, used by pg_dump to store information about schemas that are included in logical replication publications.

## Definition
```c
typedef struct _PublicationSchemaInfo
{
    DumpableObject dobj;
    PublicationInfo *publication;
    NamespaceInfo *pubschema;
} PublicationSchemaInfo;
```

## Detailed Description
This structure is part of PostgreSQL's pg_dump utility and is used to maintain the relationship between publications and the schemas they contain. When a publication is configured to replicate all tables in specific schemas (rather than individual tables), this structure stores the mapping between the publication and those schemas. This information is crucial for accurately recreating publication configurations that use schema-level replication during database dumps and restores.

## Parameters / Member Variables
- `dobj`: Base DumpableObject structure containing common metadata for dump objects
- `publication`: Pointer to the PublicationInfo structure representing the publication this schema belongs to
- `pubschema`: Pointer to the NamespaceInfo structure representing the schema that is part of the publication

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - PublicationInfo
  - [NamespaceInfo](../N/NamespaceInfo.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This structure is defined in src/bin/pg_dump/pg_dump.h at lines 661-665
- It's used specifically by the pg_dump utility for logical replication publication handling
- The struct helps maintain the many-to-many relationship between publications and schemas
- Schema-level publications were introduced to allow replicating all tables within specified schemas automatically
- This is simpler than _PublicationRelInfo as it doesn't need row filters or column lists (those are table-specific features)