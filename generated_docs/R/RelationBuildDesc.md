# RelationBuildDesc

## Location
[src/backend/utils/cache/relcache.c:1040-1319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L1040-L1319)

## Overview
Builds a complete relation descriptor from scratch by reading system catalogs and initializing all necessary components of the relation cache entry.

## Definition

```c
static Relation
RelationBuildDesc(Oid targetRelId, bool insertIt)
```
## Detailed Description
This is a core function that constructs a complete Relation structure by reading the pg_class tuple for a given relation OID and initializing all its components. The function performs extensive setup including memory management, tuple descriptor construction, access method initialization, rules/triggers/row security loading, and physical addressing setup.

The function includes sophisticated memory management with optional temporary context creation to prevent memory leaks during debug operations. It handles invalidation detection during build process and implements a retry mechanism. The function can optionally insert the completed relation into the relation cache hash table.

## Parameters / Member Variables
- `targetRelId`: OID of the relation to build descriptor for
- `insertIt`: Whether to insert the completed relation into the cache hash table
## Dependencies
- Functions called/Symbols referenced:
  - [ScanPgRelation](../S/ScanPgRelation.md) (scan pg_class for relation tuple)
  - [AllocateRelationDesc](../A/AllocateRelationDesc.md) (allocate relation structure)
  - [RelationBuildTupleDesc](RelationBuildTupleDesc.md) (build tuple descriptor)
  - [RelationInitIndexAccessInfo](RelationInitIndexAccessInfo.md) (initialize index access methods)
  - [RelationInitTableAccessMethod](RelationInitTableAccessMethod.md) (initialize table access methods)
  - [RelationParseRelOptions](RelationParseRelOptions.md) (parse relation options)
  - [RelationBuildRuleLock](RelationBuildRuleLock.md) (build rule locks)
  - [RelationBuildTriggers](RelationBuildTriggers.md) (build trigger information)
  - [RelationBuildRowSecurity](RelationBuildRowSecurity.md) (build row security policies)
  - [RelationInitLockInfo](RelationInitLockInfo.md) (initialize lock manager info)
  - [RelationInitPhysicalAddr](RelationInitPhysicalAddr.md) (initialize physical addressing)
  - RelationCacheInsert (insert into cache if requested)
  - [heap_freetuple](../h/heap_freetuple.md) (free pg_class tuple)
- Called from (representative examples):
  - [RelationIdGetRelation](RelationIdGetRelation.md)
  - [RelationClearRelation](RelationClearRelation.md)
  - [load_critical_index](../l/load_critical_index.md)

## Notes and Other Information
- Requires caller to hold at least AccessShareLock on target relation
- Returns NULL if pg_class tuple not found (relation may have been deleted)
- Implements memory recovery mechanism when debug_discard_caches is active
- Maintains in_progress_list to track invalidations during build process
- Handles different relation persistence types (permanent, unlogged, temporary)
- Properly sets up backend ownership for temporary relations
- Implements retry mechanism if invalidation occurs during build
- Initializes all relation descriptor fields including reference counts, subtransaction IDs, and validity flags
- Critical component of PostgreSQL's relation cache system

## Simplified Source

```c
static Relation RelationBuildDesc(Oid targetRelId, bool insertIt) {
    int in_progress_offset;
    Relation relation;
    HeapTuple pg_class_tuple;
    Form_pg_class relp;

    // Set up memory management for debug builds if needed
    MemoryContext tmpcxt = NULL;
    MemoryContext oldcxt = NULL;
    if (RECOVER_RELATION_BUILD_MEMORY || debug_discard_caches > 0) {
        tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
                                      "RelationBuildDesc workspace",
                                      ALLOCSET_DEFAULT_SIZES);
        oldcxt = MemoryContextSwitchTo(tmpcxt);
    }

    // Register for invalidation tracking during build
    if (in_progress_list_len >= in_progress_list_maxlen) {
        int allocsize = in_progress_list_maxlen * 2;
        in_progress_list = repalloc(in_progress_list,
                                   allocsize * sizeof(*in_progress_list));
        in_progress_list_maxlen = allocsize;
    }
    in_progress_offset = in_progress_list_len++;
    in_progress_list[in_progress_offset].reloid = targetRelId;

retry:
    in_progress_list[in_progress_offset].invalidated = false;

    // Find the pg_class tuple for this relation
    pg_class_tuple = ScanPgRelation(targetRelId, true, false);

    // Return NULL if relation not found (may have been deleted)
    if (!HeapTupleIsValid(pg_class_tuple)) {
        if (tmpcxt) {
            MemoryContextSwitchTo(oldcxt);
            MemoryContextDelete(tmpcxt);
        }
        in_progress_list_len--;
        return NULL;
    }

    // Extract relation information from pg_class tuple
    relp = (Form_pg_class) GETSTRUCT(pg_class_tuple);
    Oid relid = relp->oid;

    // Allocate and initialize relation descriptor
    relation = AllocateRelationDesc(relp);
    RelationGetRelid(relation) = relid;

    // Initialize reference counting and subtransaction tracking
    relation->rd_refcnt = 0;
    relation->rd_isnailed = false;
    relation->rd_createSubid = InvalidSubTransactionId;
    relation->rd_newRelfilelocatorSubid = InvalidSubTransactionId;
    relation->rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
    relation->rd_droppedSubid = InvalidSubTransactionId;

    // Set up backend ownership based on persistence type
    switch (relation->rd_rel->relpersistence) {
        case RELPERSISTENCE_UNLOGGED:
        case RELPERSISTENCE_PERMANENT:
            relation->rd_backend = INVALID_PROC_NUMBER;
            relation->rd_islocaltemp = false;
            break;
        case RELPERSISTENCE_TEMP:
            if (isTempOrTempToastNamespace(relation->rd_rel->relnamespace)) {
                relation->rd_backend = ProcNumberForTempRelations();
                relation->rd_islocaltemp = true;
            } else {
                // Foreign temp table - determine owning backend
                relation->rd_backend =
                    GetTempNamespaceProcNumber(relation->rd_rel->relnamespace);
                relation->rd_islocaltemp = false;
            }
            break;
        default:
            elog(ERROR, "invalid relpersistence: %c",
                 relation->rd_rel->relpersistence);
    }

    // Build tuple descriptor from pg_attribute
    RelationBuildTupleDesc(relation);

    // Initialize foreign key info (loaded on demand)
    relation->rd_fkeylist = NIL;
    relation->rd_fkeyvalid = false;

    // Initialize partitioning info (loaded on demand)
    relation->rd_partkey = NULL;
    relation->rd_partkeycxt = NULL;
    relation->rd_partdesc = NULL;
    relation->rd_partdesc_nodetached = NULL;
    relation->rd_partdesc_nodetached_xmin = InvalidTransactionId;
    relation->rd_pdcxt = NULL;
    relation->rd_pddcxt = NULL;
    relation->rd_partcheck = NIL;
    relation->rd_partcheckvalid = false;
    relation->rd_partcheckcxt = NULL;

    // Initialize access method information based on relation kind
    if (relation->rd_rel->relkind == RELKIND_INDEX ||
        relation->rd_rel->relkind == RELKIND_PARTITIONED_INDEX) {
        RelationInitIndexAccessInfo(relation);
    } else if (RELKIND_HAS_TABLE_AM(relation->rd_rel->relkind) ||
               relation->rd_rel->relkind == RELKIND_SEQUENCE) {
        RelationInitTableAccessMethod(relation);
    }

    // Parse relation options from pg_class tuple
    RelationParseRelOptions(relation, pg_class_tuple);

    // Load rules, triggers, and row security policies
    if (relation->rd_rel->relhasrules) {
        RelationBuildRuleLock(relation);
    } else {
        relation->rd_rules = NULL;
        relation->rd_rulescxt = NULL;
    }

    if (relation->rd_rel->relhastriggers) {
        RelationBuildTriggers(relation);
    } else {
        relation->trigdesc = NULL;
    }

    if (relation->rd_rel->relrowsecurity) {
        RelationBuildRowSecurity(relation);
    } else {
        relation->rd_rsdesc = NULL;
    }

    // Initialize lock manager and physical addressing info
    RelationInitLockInfo(relation);
    RelationInitPhysicalAddr(relation);
    relation->rd_smgr = NULL;

    // Clean up pg_class tuple
    heap_freetuple(pg_class_tuple);

    // Check for invalidation during build - restart if needed
    if (in_progress_list[in_progress_offset].invalidated) {
        RelationDestroyRelation(relation, false);
        goto retry;
    }
    in_progress_list_len--;

    // Insert into cache if requested
    if (insertIt)
        RelationCacheInsert(relation, true);

    relation->rd_isvalid = true;

    // Clean up temporary memory context
    if (tmpcxt) {
        MemoryContextSwitchTo(oldcxt);
        MemoryContextDelete(tmpcxt);
    }

    return relation;
}
```