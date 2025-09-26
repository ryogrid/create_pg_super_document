# _PublicationInfo

## Location
[src/bin/pg_dump/pg_dump.h:632-641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L632-L641)

## Overview
The `_PublicationInfo` structure represents logical replication publications in pg_dump, capturing all configuration settings needed to recreate publication objects during database operations.

## Definition
```c
typedef struct _PublicationInfo
{
    DumpableObject dobj;
    const char *rolname;
    bool        puballtables;
    bool        pubinsert;
    bool        pubupdate;
    bool        pubdelete;
    bool        pubtruncate;
    bool        pubviaroot;
} PublicationInfo;
```

## Detailed Description
This structure is part of pg_dump's internal representation for PostgreSQL's logical replication publications. Publications are database objects that define a set of changes that can be replicated to subscribers. The structure captures both the publication's metadata and its behavioral configuration, including which types of DML operations (INSERT, UPDATE, DELETE, TRUNCATE) should be included in the replication stream. The structure also handles the special case of publications that include all tables in the database and supports the 'publish_via_partition_root' parameter for partition table handling.

## Parameters / Member Variables
- `dobj`: Base dumpable object information containing catalog ID, name, and dump ordering details
- `rolname`: Name of the role/user that owns this publication
- `puballtables`: Boolean flag indicating whether this publication includes all tables in the database
- `pubinsert`: Boolean flag indicating whether INSERT operations are published by this publication
- `pubupdate`: Boolean flag indicating whether UPDATE operations are published by this publication  
- `pubdelete`: Boolean flag indicating whether DELETE operations are published by this publication
- `pubtruncate`: Boolean flag indicating whether TRUNCATE operations are published by this publication
- `pubviaroot`: Boolean flag for 'publish_via_partition_root' parameter, controlling whether changes to partitioned tables are published as changes to the partition root

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- This structure is defined in pg_dump.h as part of the pg_dump utility's internal data structures
- The typedef creates an alias `PublicationInfo` for easier reference throughout the codebase
- Publications are central to PostgreSQL's logical replication feature, which allows selective replication of data changes
- The boolean flags correspond to the WITH clause options when creating publications (e.g., WITH (publish = 'insert,update'))
- The `pubviaroot` setting affects how partitioned tables are handled in replication - [when](../w/when.md) true, changes are published as if they occurred on the partition root table
- The structure enables pg_dump to preserve complete publication configurations across database migrations and logical replication setup recreation