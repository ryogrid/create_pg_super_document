# pg_stat_reset_single_table_counters

## Location
src/backend/utils/adt/pgstatfuncs.c: 1750 - 1760

## Overview
A PostgreSQL system function that resets statistics for a single table or relation, handling both regular database tables and shared system relations across the entire cluster.

## Definition


## Detailed Description
The  function provides a targeted mechanism to reset statistical counters for a specific table or relation identified by its OID (Object Identifier). The function intelligently determines whether the target relation is a shared system relation (accessible across all databases) or a regular table within the current database, and resets the appropriate statistics accordingly.

For shared relations, the function uses InvalidOid as the database ID, indicating that the statistics apply cluster-wide. For regular tables, it uses the current database ID (MyDatabaseId) to scope the statistics reset to the current database context.

## Parameters / Member Variables
-  (Oid): The Object Identifier of the table/relation whose statistics should be reset. This parameter is required and obtained from the first function argument.

## Dependencies  
- Functions called/Symbols referenced:
  - [IsSharedRelation](../I/IsSharedRelation.md) (to determine if the relation is shared across databases)
  - [pgstat_reset](pgstat_reset.md) (to perform the actual statistics reset)
  - PG_RETURN_VOID (to return from the function)
- Constants used:
  - PGSTAT_KIND_RELATION (specifies that relation statistics are being reset)
  - InvalidOid (used for shared relations to indicate cluster-wide scope)
  - MyDatabaseId (current database identifier for non-shared relations)
- Called from:
  - SQL function interface (no direct C callers found)

## Notes and Other Information
- This function is designed to handle both regular database tables and shared system relations (like system catalogs that exist across all databases)
- The function automatically determines the appropriate database scope based on whether the relation is shared
- Statistics reset is performed through the general  mechanism using the PGSTAT_KIND_RELATION category
- The function requires appropriate privileges to execute, as it affects table-level statistics
- Unlike  which handles cluster-wide statistics categories, this function focuses on individual table/relation statistics
- The OID parameter must correspond to a valid relation; invalid OIDs will be handled by the underlying pgstat_reset function