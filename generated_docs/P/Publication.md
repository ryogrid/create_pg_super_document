# Publication

## Location
[src/include/catalog/pg_publication.h:100-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/pg_publication.h#L100-L107)

## Overview
Publication is the main structure representing a logical replication publication object in PostgreSQL, containing its identity, configuration, and operational settings.

## Definition

```c
typedef struct Publication
{
	Oid			oid;
	char	   *name;
	bool		alltables;
	bool		pubviaroot;
	PublicationActions pubactions;
} Publication;
```
## Detailed Description
Publication is the primary data structure that represents a logical replication publication in PostgreSQL. It encapsulates all the essential information needed to define and manage a publication, including its unique identifier, name, scope configuration, and the types of operations it publishes.

This structure serves as the in-memory representation of publication objects and is used throughout the logical replication system for managing publication metadata, determining which tables and operations to replicate, and configuring replication behavior. The structure combines basic identification (OID and name) with behavioral flags and operation controls.

The Publication structure is fundamental to PostgreSQL's logical replication architecture, acting as the authoritative source of publication configuration that drives replication decisions and table inclusion logic.

## Parameters / Member Variables
- `oid`: Object identifier (OID) of the publication in the system catalog
- `*name`: String name of the publication as specified by the user
- `alltables`: Boolean flag indicating whether the publication includes all tables in the database
- `pubviaroot`: Boolean flag controlling whether partitioned tables are published via their root table or individual partitions
- `pubactions`: PublicationActions structure specifying which DML operations (INSERT, UPDATE, DELETE, TRUNCATE) are published
## Dependencies
- Functions called/Symbols referenced:
  - [PublicationActions](PublicationActions.md) (embedded structure at line 106)
- Called from (representative examples):
  - [get_object_address_publication_rel](../g/get_object_address_publication_rel.md) (src/backend/catalog/objectaddress.c:1870)
  - [publication_add_relation](../p/publication_add_relation.md) (src/backend/catalog/pg_publication.c:368)
  - [GetPublication](../G/GetPublication.md) (src/backend/catalog/pg_publication.c:1009)
  - [LoadPublications](../L/LoadPublications.md) (src/backend/replication/pgoutput/pgoutput.c:1754)
  - [get_rel_sync_entry](../g/get_rel_sync_entry.md) (src/backend/replication/pgoutput/pgoutput.c:2139)

## Notes and Other Information
- This structure represents the runtime/cache version of publication data, distinct from the catalog storage format
- The 'alltables' flag provides a mechanism for wholesale table inclusion without explicitly listing each table
- The 'pubviaroot' flag is particularly important for partitioned table hierarchies, determining replication granularity
- Publications are identified both by OID (for internal system use) and by name (for user interaction)
- The structure is used extensively in the pgoutput plugin for logical replication decision-making
- Memory management for the 'name' field should be handled appropriately by the calling context