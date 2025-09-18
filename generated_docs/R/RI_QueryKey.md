# RI_QueryKey

## Location
src/backend/utils/adt/ri_triggers.c: 132 - 136

## Overview
RI_QueryKey is a structure that serves as the key for identifying prepared SPI plans in the referential integrity query hashtable, enabling efficient lookup and caching of SQL query plans used for foreign key constraint checking.

## Definition


## Detailed Description
RI_QueryKey provides a composite key mechanism for the query plan cache used in referential integrity operations. Each key uniquely identifies a specific type of query plan for a particular foreign key constraint. The structure combines the constraint identifier with a query type code to ensure that different types of operations on the same constraint (such as CASCADE DELETE vs RESTRICT operations) can be cached separately and retrieved efficiently.

## Parameters / Member Variables
- : OID of the pg_constraint entry that this query plan is associated with
- : Query type identifier that specifies the kind of referential integrity operation (see RI_PLAN_XXX constants)

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - int32 (32-bit integer type)
- Called from (representative examples):
  - RI_QueryHashEntry (as a member)
  - ri_BuildQueryKey
  - ri_FetchPreparedPlan
  - ri_HashPreparedPlan
  - ri_PlanCheck
  - ri_PerformCheck

## Notes and Other Information
The constr_queryno field references various RI_PLAN_XXX constants that define different types of referential integrity queries:
- RI_PLAN_CHECK_LOOKUPPK (1): Check lookup against primary key table
- RI_PLAN_CHECK_LOOKUPPK_FROM_PK (2): Check lookup from primary key table
- RI_PLAN_CASCADE_ONDELETE (3): Cascade delete operation
- RI_PLAN_CASCADE_ONUPDATE (4): Cascade update operation  
- RI_PLAN_RESTRICT (5): Restrict operation (same for DELETE and UPDATE)
- RI_PLAN_SETNULL_ONDELETE (6): Set null on delete operation
- RI_PLAN_SETNULL_ONUPDATE (7): Set null on update operation
- RI_PLAN_SETDEFAULT_ONDELETE (8): Set default on delete operation
- RI_PLAN_SETDEFAULT_ONUPDATE (9): Set default on update operation

This key structure is essential for PostgreSQL's query plan caching system for referential integrity, allowing the database to avoid re-planning the same types of constraint checking queries repeatedly.