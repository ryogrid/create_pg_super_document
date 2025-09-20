# get_template0_info

## Location
[src/bin/pg_upgrade/info.c:314-378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/info.c#L314-L378)

## Overview
Retrieves locale and encoding information from the template0 database, which serves as the base template that will be copied from the old cluster to the new cluster during PostgreSQL upgrades.

## Definition

```c
static void
get_template0_info(ClusterInfo *cluster)
```
## Detailed Description
This static function connects to the template1 database to query information about template0, which is crucial for maintaining locale and encoding consistency during PostgreSQL upgrades. The function handles version-specific differences in how locale information is stored and accessed across different PostgreSQL major versions.

The function adapts its query based on the PostgreSQL version, accommodating changes in locale provider functionality and column names. For PostgreSQL 17.0+, it uses the standard datlocale field; for 15.0-16.x, it uses daticulocale aliased as datlocale; and for older versions, it provides compatibility by using hardcoded values and NULL for unsupported fields.

The retrieved information is stored in the cluster's template0 field for later use during the upgrade process, ensuring that new databases created in the target cluster maintain the same locale characteristics as the source cluster.

## Parameters / Member Variables
- : Pointer to ClusterInfo structure representing the PostgreSQL cluster being analyzed

## Dependencies
- Functions called/Symbols referenced:
  - [connectToServer](../c/connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - GET_MAJOR_VERSION
  - pg_malloc
  - [pg_strdup](../p/pg_strdup.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQclear](../P/PQclear.md)
  - [PQfinish](../P/PQfinish.md)
  - [pg_fatal](../p/pg_fatal.md)
  - atoi
- Data structures used:
  - ClusterInfo
  - DbLocaleInfo
  - PGconn
  - PGresult
- Called from (representative examples):
  - [get_db_rel_and_slot_infos](get_db_rel_and_slot_infos.md)

## Notes and Other Information
- Static function - only accessible within the same source file (info.c)
- Connects to template1 database to query information about template0 database
- Handles PostgreSQL version compatibility for locale provider changes introduced in version 15.0
- Uses different query strategies based on major version: 17.0+, 15.0-16.x, and pre-15.0
- Stores encoding, collation provider, collate, ctype, and locale information
- Critical for maintaining database locale consistency during pg_upgrade operations
- Memory allocated for DbLocaleInfo must be managed by the calling code
- Part of pg_upgrade's cluster information gathering infrastructure