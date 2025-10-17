# formrdesc

## Location
[src/backend/utils/cache/relcache.c:1875-2062](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L1875-L2062)

## Overview
A special cut-down version of RelationBuildDesc() used during relcache initialization to build relation descriptors from supplied parameters without accessing system tables.

## Definition

```c
enumber they currently
	 * have.  In bootstrap mode, add them to the initial relation mapper data,
	 * specifying that the initial filenumber is the same as the OID.
	 */
	relation->rd_rel->relfilenode = InvalidRelFileNumber;
```
## Detailed Description
This function creates relation descriptors for basic system catalogs during PostgreSQL's bootstrap phase and early initialization. It builds the relation descriptor purely from the provided parameters without performing any system table lookups, making it suitable for use when the system catalogs themselves are not yet fully available.

The function handles the creation of "nailed" relations (permanently cached relations) and sets up minimal but sufficient information to get the system launched. The actual complete data will be filled in later by RelationCacheInitializePhase3().

Key characteristics of relations created by formrdesc:
- All are marked as nailed-in-cache with reference count 1
- All are permanent relations (RELPERSISTENCE_PERMANENT)
- All use heap table access method (HEAP_TABLE_AM_OID)
- All are mapped relations with invalid file numbers initially
- Cannot have constraints (except attnotnull), default values, rules, or triggers

## Parameters / Member Variables
- : Name of the relation to create
- : OID of the relation's composite type
- : Whether this is a shared relation (stored in global tablespace)
- : Number of attributes in the relation
- : Array of attribute definitions (FormData_pg_attribute)

## Dependencies
- Functions called/Symbols referenced:
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [RelationInitLockInfo](../R/RelationInitLockInfo.md)
  - [RelationInitPhysicalAddr](../R/RelationInitPhysicalAddr.md)
  - [GetHeapamTableAmRoutine](../G/GetHeapamTableAmRoutine.md)
  - [RelationMapUpdateMap](../R/RelationMapUpdateMap.md)
  - RelationCacheInsert
  - IsBootstrapProcessingMode
  - [namestrcpy](../n/namestrcpy.md)
- Called from (representative examples):
  - [RelationCacheInitializePhase2](../R/RelationCacheInitializePhase2.md)
  - [RelationCacheInitializePhase3](../R/RelationCacheInitializePhase3.md)

## Notes and Other Information
- Only used for a few basic system catalogs during initialization
- Creates incomplete/bogus data that gets replaced later in RelationCacheInitializePhase3()
- The relowner field is left as zero to signal that real data isn't loaded yet
- All relations created are marked as having indexes (except in bootstrap mode)
- Assumes caller has already switched to CacheMemoryContext
- Part of PostgreSQL's three-phase relation cache initialization process
- Critical for bootstrap process when system catalogs are being created

## Simplified Source

```c
static void formrdesc(const char *relationName, Oid relationReltype,
                     bool isshared, int natts, const FormData_pg_attribute *attrs) {
    // Allocate and initialize relation descriptor
    Relation relation = (Relation) palloc0(sizeof(RelationData));
    relation->rd_smgr = NULL;
    relation->rd_refcnt = 1;  // Nailed in cache

    // Mark as nailed relation (permanently cached)
    relation->rd_isnailed = true;
    relation->rd_createSubid = InvalidSubTransactionId;
    relation->rd_newRelfilelocatorSubid = InvalidSubTransactionId;
    relation->rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
    relation->rd_droppedSubid = InvalidSubTransactionId;
    relation->rd_backend = INVALID_PROC_NUMBER;
    relation->rd_islocaltemp = false;

    // Initialize pg_class tuple form (minimal bootstrap data)
    relation->rd_rel = (Form_pg_class) palloc0(CLASS_TUPLE_SIZE);
    namestrcpy(&relation->rd_rel->relname, relationName);
    relation->rd_rel->relnamespace = PG_CATALOG_NAMESPACE;
    relation->rd_rel->reltype = relationReltype;

    // Set shared/non-shared status and tablespace
    relation->rd_rel->relisshared = isshared;
    if (isshared)
        relation->rd_rel->reltablespace = GLOBALTABLESPACE_OID;

    // All formrdesc relations are permanent and populated
    relation->rd_rel->relpersistence = RELPERSISTENCE_PERMANENT;
    relation->rd_rel->relispopulated = true;
    relation->rd_rel->relreplident = REPLICA_IDENTITY_NOTHING;
    relation->rd_rel->relpages = 0;
    relation->rd_rel->reltuples = -1;
    relation->rd_rel->relallvisible = 0;
    relation->rd_rel->relkind = RELKIND_RELATION;
    relation->rd_rel->relnatts = (int16) natts;
    relation->rd_rel->relam = HEAP_TABLE_AM_OID;

    // Build tuple descriptor from provided attributes
    relation->rd_att = CreateTemplateTupleDesc(natts);
    relation->rd_att->tdrefcount = 1;
    relation->rd_att->tdtypeid = relationReltype;
    relation->rd_att->tdtypmod = -1;

    // Copy attribute definitions and check for not-null constraints
    bool has_not_null = false;
    for (int i = 0; i < natts; i++) {
        memcpy(TupleDescAttr(relation->rd_att, i), &attrs[i],
               ATTRIBUTE_FIXED_PART_SIZE);
        has_not_null |= attrs[i].attnotnull;
        TupleDescAttr(relation->rd_att, i)->attcacheoff = -1;
    }

    // Optimize first attribute cache offset
    TupleDescAttr(relation->rd_att, 0)->attcacheoff = 0;

    // Set up constraint info if any not-null constraints exist
    if (has_not_null) {
        TupleConstr *constr = (TupleConstr *) palloc0(sizeof(TupleConstr));
        constr->has_not_null = true;
        relation->rd_att->constr = constr;
    }

    // Extract relation OID from first attribute
    RelationGetRelid(relation) = TupleDescAttr(relation->rd_att, 0)->attrelid;

    // All formrdesc relations are mapped relations
    relation->rd_rel->relfilenode = InvalidRelFileNumber;
    if (IsBootstrapProcessingMode())
        RelationMapUpdateMap(RelationGetRelid(relation),
                           RelationGetRelid(relation), isshared, true);

    // Initialize lock manager and physical addressing
    RelationInitLockInfo(relation);
    RelationInitPhysicalAddr(relation);

    // Set up heap table access method
    relation->rd_rel->relam = HEAP_TABLE_AM_OID;
    relation->rd_tableam = GetHeapamTableAmRoutine();

    // Set index flag based on bootstrap mode
    if (IsBootstrapProcessingMode()) {
        relation->rd_rel->relhasindex = false;
    } else {
        relation->rd_rel->relhasindex = true;
    }

    // Insert into cache and mark as valid
    RelationCacheInsert(relation, false);
    relation->rd_isvalid = true;
}
```