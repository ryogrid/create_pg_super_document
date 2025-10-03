# pgstat_drop_database

## Location
[src/backend/utils/activity/pgstat_database.c:44-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_database.c#L44-L54)

## Overview
Removes the statistics entry for a database when it is being dropped from the PostgreSQL cluster.

## Definition

```c
void
pgstat_drop_database(Oid databaseid)
```
## Detailed Description
This function is responsible for cleaning up statistics tracking data when a database is dropped. It acts as a wrapper around the more general  function, specifically targeting database-level statistics. The function ensures that statistics information for the dropped database is properly removed from the system's statistics collection framework, preventing memory leaks and stale data from persisting after database removal.

## Parameters / Member Variables
- `databaseid`: The OID (Object Identifier) of the database being dropped
## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_drop_transactional](pgstat_drop_transactional.md)
  - PGSTAT_KIND_DATABASE
- Called from (representative examples):
  - [dropdb](../d/dropdb.md) (in src/backend/commands/dbcommands.c:1777)

## Notes and Other Information
- This function is part of PostgreSQL's statistics collection system
- Located in src/backend/utils/activity/pgstat_database.c:44-54
- Uses the transactional statistics dropping mechanism to ensure consistency
- The function is called during database drop operations to maintain clean statistics state

## Simplified Source

```c
void pgstat_drop_database(Oid databaseid) {
    // Remove database statistics entry transactionally
    pgstat_drop_transactional(PGSTAT_KIND_DATABASE, databaseid, InvalidOid);
}
```