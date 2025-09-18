# RelationBuildPublicationDesc

## Location
src/backend/utils/cache/relcache.c: 5728 - 5875

## Overview
Builds and caches publication information for a relation, including publication actions (insert/update/delete/truncate) and validation status for row filters and column lists in logical replication.

## Definition


## Detailed Description
This function constructs a comprehensive publication descriptor for a given relation by traversing all publications that include the relation. It consolidates publication actions and validates row filter expressions and column lists for logical replication.

The function performs the following key operations:

1. **Publishability Check**: First verifies if the relation is publishable using 
2. **Cache Check**: Returns cached information if already available in 
3. **Publication Discovery**: Gathers all relevant publications by:
   - Getting direct relation publications via 
   - Adding schema-level publications via 
   - For partitioned tables, including ancestor publications via 
   - Adding "FOR ALL TABLES" publications via 

4. **Action Consolidation**: For each publication, it performs bitwise OR operations to accumulate:
   - , , ,  actions

5. **Validation Checks**: Validates row filters and column lists by calling:
   -  - ensures row filter expressions only reference REPLICA IDENTITY columns
   -  - ensures column lists only include REPLICA IDENTITY columns

6. **Optimization**: Breaks early if all actions are enabled and validation flags are set to false
7. **Caching**: Stores the result in  for future use

## Parameters / Member Variables
- : The relation to build publication description for
- : Output parameter - populated with publication actions and validation status

## Dependencies
- Functions called/Symbols referenced:
  - [is_publishable_relation](../i/is_publishable_relation.md)
  - [GetRelationPublications](../G/GetRelationPublications.md)
  - RelationGetNamespace/GetSchemaPublications
  - [get_partition_ancestors](../g/get_partition_ancestors.md)/get_rel_namespace
  - [GetAllTablesPublications](../G/GetAllTablesPublications.md)
  - [list_concat_unique_oid](../l/list_concat_unique_oid.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)/ReleaseSysCache
  - [pub_rf_contains_invalid_column](../p/pub_rf_contains_invalid_column.md)
  - [pub_collist_contains_invalid_column](../p/pub_collist_contains_invalid_column.md)
  - Form_pg_publication
- Called from (representative examples):
  - [CheckCmdReplicaIdentity](../C/CheckCmdReplicaIdentity.md)

## Notes and Other Information
- Results are cached in the relation cache entry for performance optimization
- For non-publishable relations, all validation flags are set to true by default
- Handles partitioned tables by including publications from ancestor relations
- Row filter and column list validation only applies to non-"FOR ALL TABLES" publications
- Early termination optimization when all actions are determined and validation is complete
- Uses  for storing cached publication descriptors
- Critical for logical replication to ensure data consistency and security