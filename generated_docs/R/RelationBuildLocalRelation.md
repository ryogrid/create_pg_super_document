# RelationBuildLocalRelation

## Location
[src/backend/utils/cache/relcache.c:3526-3768](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L3526-L3768)

## Overview
RelationBuildLocalRelation builds a relcache entry for a relation that is about to be created and enters it into the relcache, providing in-memory representation before the relation physically exists on disk.

## Definition

```c
Relation
RelationBuildLocalRelation(const char *relname,
						   Oid relnamespace,
						   TupleDesc tupDesc,
						   Oid relid,
						   Oid accessmtd,
						   RelFileNumber relfilenumber,
						   Oid reltablespace,
						   bool shared_relation,
						   bool mapped_relation,
						   char relpersistence,
						   char relkind)
```
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
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - [IsSharedRelation](../I/IsSharedRelation.md)
  - [CreateCacheMemoryContext](../C/CreateCacheMemoryContext.md)
  - [RelationMapUpdateMap](RelationMapUpdateMap.md)
  - [RelationInitPhysicalAddr](RelationInitPhysicalAddr.md)
  - [RelationInitTableAccessMethod](RelationInitTableAccessMethod.md)
  - RelationCacheInsert
  - EOXactListAdd
  - [RelationIncrementReferenceCount](RelationIncrementReferenceCount.md)
- Called from (representative examples):
  - [heap_create](../h/heap_create.md)

## Notes and Other Information
- The function includes validation that shared_relation matches IsSharedRelation() to ensure consistency with hardcoded shared relation lists
- System catalogs (DatabaseRelationId, RelationRelationId, etc.) are automatically nailed in cache for performance
- Materialized views are initially marked as unpopulated since they require explicit refresh
- The relation is marked with rd_createSubid for proper transaction cleanup
- Replica identity is set to DEFAULT for user tables but NOTHING for system catalogs
- The function switches memory contexts to ensure proper allocation in CacheMemoryContext
- Returns a pinned relation reference that the caller must eventually release

## Simplified Source

```c
Relation
RelationBuildLocalRelation(const char *relname,
                          Oid relnamespace,
                          TupleDesc tupDesc,
                          Oid relid,
                          Oid accessmtd,
                          RelFileNumber relfilenumber,
                          Oid reltablespace,
                          bool shared_relation,
                          bool mapped_relation,
                          char relpersistence,
                          char relkind)
{
    Relation rel;
    MemoryContext oldcxt;
    bool nailit;

    // Check if this relation should be nailed in cache (system catalogs)
    nailit = (relid == DatabaseRelationId || relid == AuthIdRelationId ||
              relid == RelationRelationId || relid == AttributeRelationId ||
              relid == ProcedureRelationId || relid == TypeRelationId);

    // Validate shared relation flag consistency
    if (shared_relation != IsSharedRelation(relid))
        elog(ERROR, "shared_relation flag mismatch for relation %s", relname);

    // Switch to cache memory context for persistent allocation
    if (!CacheMemoryContext)
        CreateCacheMemoryContext();
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

    // Allocate and initialize relation descriptor
    rel = (Relation) palloc0(sizeof(RelationData));
    rel->rd_smgr = NULL;
    rel->rd_isnailed = nailit;
    rel->rd_refcnt = nailit ? 1 : 0;
    rel->rd_createSubid = GetCurrentSubTransactionId();

    // Create tuple descriptor copy and preserve attribute properties
    rel->rd_att = CreateTupleDescCopy(tupDesc);
    rel->rd_att->tdrefcount = 1;

    // Set up NOT NULL constraints if present
    bool has_not_null = false;
    for (int i = 0; i < tupDesc->natts; i++) {
        Form_pg_attribute satt = TupleDescAttr(tupDesc, i);
        Form_pg_attribute datt = TupleDescAttr(rel->rd_att, i);

        datt->attnotnull = satt->attnotnull;
        has_not_null |= satt->attnotnull;
    }

    if (has_not_null) {
        TupleConstr *constr = (TupleConstr *) palloc0(sizeof(TupleConstr));
        constr->has_not_null = true;
        rel->rd_att->constr = constr;
    }

    // Initialize relation tuple form (pg_class data)
    rel->rd_rel = (Form_pg_class) palloc0(CLASS_TUPLE_SIZE);
    namestrcpy(&rel->rd_rel->relname, relname);
    rel->rd_rel->relnamespace = relnamespace;
    rel->rd_rel->relkind = relkind;
    rel->rd_rel->relnatts = tupDesc->natts;
    rel->rd_rel->relpersistence = relpersistence;

    // Set persistence-dependent fields
    switch (relpersistence) {
        case RELPERSISTENCE_TEMP:
            rel->rd_backend = ProcNumberForTempRelations();
            rel->rd_islocaltemp = true;
            break;
        default:
            rel->rd_backend = INVALID_PROC_NUMBER;
            rel->rd_islocaltemp = false;
            break;
    }

    // Handle materialized views (initially unpopulated)
    rel->rd_rel->relispopulated = (relkind != RELKIND_MATVIEW);

    // Set replica identity
    if (!IsCatalogNamespace(relnamespace) &&
        (relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW))
        rel->rd_rel->relreplident = REPLICA_IDENTITY_DEFAULT;
    else
        rel->rd_rel->relreplident = REPLICA_IDENTITY_NOTHING;

    // Set physical identifiers and file mapping
    rel->rd_rel->relisshared = shared_relation;
    RelationGetRelid(rel) = relid;
    rel->rd_rel->reltablespace = reltablespace;

    if (mapped_relation) {
        rel->rd_rel->relfilenode = InvalidRelFileNumber;
        RelationMapUpdateMap(relid, relfilenumber, shared_relation, true);
    } else {
        rel->rd_rel->relfilenode = relfilenumber;
    }

    // Initialize locking and physical address
    RelationInitLockInfo(rel);
    RelationInitPhysicalAddr(rel);
    rel->rd_rel->relam = accessmtd;

    // Switch back to original context for table access method init
    MemoryContextSwitchTo(oldcxt);

    if (RELKIND_HAS_TABLE_AM(relkind) || relkind == RELKIND_SEQUENCE)
        RelationInitTableAccessMethod(rel);

    // Insert into relcache and mark for transaction cleanup
    RelationCacheInsert(rel, nailit);
    EOXactListAdd(rel);
    rel->rd_isvalid = true;

    // Pin and return the relation
    RelationIncrementReferenceCount(rel);
    return rel;
}
```