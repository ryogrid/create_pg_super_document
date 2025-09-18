# pg_partition_root

## Location
src/backend/utils/adt/partitionfuncs.c: 164 - 200

## Overview
A PostgreSQL system function that finds and returns the top-most parent (root) relation in a partition tree hierarchy for a given relation OID.

## Definition


## Detailed Description
This function implements the  SQL function that traverses a partition hierarchy upward to locate the root partitioned table. It serves as a key utility in PostgreSQL's declarative partitioning system, allowing users and internal operations to identify the ultimate parent table in a partition tree.

The function works by:
1. Validating that the input relation can participate in partition trees
2. Retrieving the complete list of ancestors using 
3. Returning either the input relation itself (if it's already the root) or the topmost ancestor

If the input relation is not part of a partition tree or cannot be processed as a partition, the function returns NULL.

## Parameters / Member Variables
- Function follows PostgreSQL's standard function argument pattern using 
- Input parameter (accessed via ):
  - : OID of the relation for which to find the partition root

## Dependencies
- Functions called/Symbols referenced:
  - check_rel_can_be_partition: Validates if the relation can participate in partition trees
  - get_partition_ancestors: Retrieves the list of ancestor relations in the partition hierarchy
  - llast_oid: Gets the last (topmost) OID from the ancestors list
  - list_free: Frees the memory allocated for the ancestors list
  - PG_RETURN_OID: PostgreSQL macro for returning an OID value
  - PG_RETURN_NULL: PostgreSQL macro for returning NULL

- Called from (representative examples):
  - Available as SQL function  for end-user queries
  - Used internally by partition management operations

## Notes and Other Information
- This function is exposed as a SQL system function, making it callable from SQL queries
- Returns NULL for non-partition relations or invalid inputs, providing graceful error handling
- Uses an assertion to verify that a valid root OID is found when the input relation is confirmed to be part of a partition tree
- Efficient implementation that avoids unnecessary traversal when the input is already the root
- Part of PostgreSQL's information schema functions for introspecting partition hierarchies
- Memory management is handled properly by freeing the ancestors list after use
- Critical for partition maintenance operations and query planning in partitioned environments