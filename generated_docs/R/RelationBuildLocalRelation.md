# RelationBuildLocalRelation

## Location
src/backend/utils/cache/relcache.c: 3526 - 3768

## Overview
RelationBuildLocalRelation builds a relcache entry for a relation that is about to be created and enters it into the relcache, providing in-memory representation before the relation physically exists on disk.

## Definition


## Detailed Description
This function creates a complete relation cache entry for a relation that is being created within the current transaction. It performs several critical tasks:

1. **Memory Management**: Allocates the relation structure in CacheMemoryContext to ensure it persists beyond the current transaction
2. **Tuple Descriptor Setup**: Creates a copy of the provided tuple descriptor, preserving attribute properties like attnotnull, attidentity, and attgenerated
3. **Relation Metadata Initialization**: Sets up the Form_pg_class structure with basic relation properties including name, namespace, kind, and persistence
4. **Nailing Logic**: Determines if the relation should be "nailed" in cache (kept permanently loaded) based on system catalog OIDs
5. **Storage Mapping**: Handles both regular and mapped relations, updating the relation mapping for mapped relations
6. **Transaction Tracking**: Records the creating subtransaction ID for proper cleanup at transaction end
7. **Cache Integration**: Inserts the relation into the relcache hash table and marks it for end-of-transaction cleanup

The function handles different relation types (permanent, temporary, unlogged) and ensures proper backend assignment for temporary relations. For materialized views, it correctly sets the initially unpopulated state.

## Parameters / Member Variables
- : Name of the relation being created
- : OID of the namespace (schema) containing the relation  
- : Tuple descriptor defining the relation's column structure
- : OID assigned to the new relation
- : OID of the table access method (for tables/sequences)
- : Physical file number for relation storage
- : OID of the tablespace where relation will be stored
- : Whether this is a cluster-wide shared relation
- : Whether this relation uses the relation mapping mechanism
- : Persistence level (permanent, temporary, or unlogged)
- : Type of relation (table, index, sequence, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentSubTransactionId
  - CreateTupleDescCopy
  - IsSharedRelation
  - CreateCacheMemoryContext
  - RelationMapUpdateMap
  - RelationInitPhysicalAddr
  - RelationInitTableAccessMethod
  - RelationCacheInsert
  - EOXactListAdd
  - RelationIncrementReferenceCount
- Called from (representative examples):
  - heap_create

## Notes and Other Information
- The function includes validation that shared_relation matches IsSharedRelation() to ensure consistency with hardcoded shared relation lists
- System catalogs (DatabaseRelationId, RelationRelationId, etc.) are automatically nailed in cache for performance
- Materialized views are initially marked as unpopulated since they require explicit refresh
- The relation is marked with rd_createSubid for proper transaction cleanup
- Replica identity is set to DEFAULT for user tables but NOTHING for system catalogs
- The function switches memory contexts to ensure proper allocation in CacheMemoryContext
- Returns a pinned relation reference that the caller must eventually release