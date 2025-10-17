# ri_BuildQueryKey

## Location
[src/backend/utils/adt/ri_triggers.c:1980-2011](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L1980-L2011)

## Overview
A utility function that constructs a hashtable key for identifying cached prepared SPI plans used in foreign key constraint operations, with optimization for inherited constraints.

## Definition

```c
struct RI_QueryKey contains no padding bytes, else we'd need
	 * to use memset to clear them.
	 */
	if (constr_queryno != RI_PLAN_CHECK_LOOKUPPK_FROM_PK)
		key->constr_id = riinfo->constraint_root_id;
```
## Detailed Description
This function builds a hashtable key used to identify and cache prepared SPI (Server Programming Interface) plans for foreign key constraint operations. The key enables efficient plan reuse across multiple executions of the same type of referential integrity query. 

The function implements an important optimization for inherited constraints: when multiple partitions share a common ancestor constraint, they can reuse the same cached SPI plan for most query types (except RI_PLAN_CHECK_LOOKUPPK_FROM_PK). This optimization works because most FK constraint queries process the "other" table in the relationship (not the table where the trigger fired), making the query identical across inheritance hierarchy members. The function uses the root constraint's OID rather than the individual constraint's OID in such cases, significantly reducing memory usage and plan compilation overhead in partitioned environments.

For the special case of RI_PLAN_CHECK_LOOKUPPK_FROM_PK queries, the function uses the specific constraint's OID since these queries are table-specific and cannot be shared across partitions.

## Parameters / Member Variables
- : Output parameter; pointer to RI_QueryKey structure that will be filled with the constructed hashtable key
- : Pointer to RI_ConstraintInfo structure containing information derived from the pg_constraint catalog entry
- : Internal query type identifier (corresponds to RI_PLAN_XXX constants) that specifies which type of referential integrity operation this key represents

## Dependencies
- Functions called/Symbols referenced:
  - : Structure type used for the hashtable key
  - : Structure containing constraint information from system catalogs
  - : Constant identifying the special case query type that cannot share plans across inheritance

- Called from (representative examples):
  - : Used for primary key matching operations
  - : Used in foreign key restriction checks  
  - : Used in foreign key cascade delete operations
  - : Used in foreign key cascade update operations
  - : Used in referential integrity set operations

## Notes and Other Information
- This is a static function within ri_triggers.c, specifically designed for the referential integrity caching system
- The function assumes that the RI_QueryKey structure contains no padding bytes, eliminating the need for memset initialization
- Critical for performance in partitioned environments where many partitions have similar foreign key constraints
- The optimization reduces both memory usage and query compilation overhead by maximizing plan reuse
- Each partition still requires its own RI_ConstraintInfo structure due to potential differences in column ordering (pk_attnums[] and fk_attnums[] arrays)
- The distinction between constraint_root_id and constraint_id enables the inheritance optimization while maintaining correctness for table-specific operations
- Essential component of PostgreSQL's SPI plan caching infrastructure for referential integrity enforcement

## Simplified Source

```c
static void
ri_BuildQueryKey(RI_QueryKey *key, const RI_ConstraintInfo *riinfo, int32 constr_queryno)
{
    // Optimization for inherited constraints: share plans across partitions
    // except for RI_PLAN_CHECK_LOOKUPPK_FROM_PK queries
    if (constr_queryno != RI_PLAN_CHECK_LOOKUPPK_FROM_PK) {
        // Use root constraint ID for plan sharing across inheritance hierarchy
        key->constr_id = riinfo->constraint_root_id;
    } else {
        // Use specific constraint ID for table-specific queries
        key->constr_id = riinfo->constraint_id;
    }

    // Set the query type identifier
    key->constr_queryno = constr_queryno;
}
```