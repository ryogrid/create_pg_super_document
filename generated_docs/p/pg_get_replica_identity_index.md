# pg_get_replica_identity_index

## Location
src/backend/utils/adt/misc.c: 1101 - 1120

## Overview
An SQL-accessible function that returns the OID of the replica identity index for a given relation, used in PostgreSQL's logical replication system.

## Definition


## Detailed Description
This function serves as an SQL wrapper around the internal  function. It retrieves the replica identity index for a specified table, which is crucial for PostgreSQL's logical replication functionality. The replica identity index determines which index is used to identify rows uniquely for replication purposes when changes need to be replicated to subscriber databases.

The function opens the relation with an AccessShareLock, retrieves the replica identity index OID through , and then closes the relation. If a valid replica identity index exists, it returns the index OID; otherwise, it returns NULL.

## Parameters / Member Variables
- : OID of the relation (table) for which to find the replica identity index

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (to extract the relation OID parameter)
  - table_open (to open the relation with AccessShareLock)
  - RelationGetReplicaIndex (core function that retrieves the replica identity index)
  - table_close (to close the relation and release the lock)
  - OidIsValid (to check if the returned OID is valid)
  - PG_RETURN_OID (to return the index OID)
  - PG_RETURN_NULL (to return NULL when no index exists)
- Called from (representative examples):
  - SQL queries accessing system functions
  - Logical replication related operations

## Notes and Other Information
- Returns NULL if no replica identity index is configured for the relation
- Used primarily in logical replication contexts where row identification is crucial
- The function uses AccessShareLock, making it safe for concurrent operations
- Located in src/backend/utils/adt/misc.c:1101-1120
- Part of PostgreSQL's system function interface for accessing internal relation metadata