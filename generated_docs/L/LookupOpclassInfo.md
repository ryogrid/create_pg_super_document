# LookupOpclassInfo

## Location
[src/backend/utils/cache/relcache.c:1648-1800](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L1648-L1800)

## Overview
LookupOpclassInfo maintains a per-operator-class cache of support procedure information needed for index operations, providing efficient access to operator class metadata without repeated catalog scans.

## Definition
```c
static OpClassCacheEnt *LookupOpclassInfo(Oid operatorClassOid,
                                          StrategyNumber numSupport)
```

## Detailed Description
This static function implements a caching mechanism for operator class information used by IndexSupportInitialize(). It maintains a hash table (OpClassCache) that stores OpClassCacheEnt structures containing operator family, input type, and support procedure information for each operator class. When called, it either returns cached information or performs catalog scans of pg_opclass and pg_amproc to populate a new cache entry. The function handles bootstrap scenarios by forcing heap scans for critical operator classes to avoid infinite recursion during system startup.

## Parameters / Member Variables
- `operatorClassOid`: The OID of the operator class to look up
- `numSupport`: Expected number of support procedures for this operator class (from access method)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md), hash_search
  - [CreateCacheMemoryContext](../C/CreateCacheMemoryContext.md), MemoryContextAllocZero
  - [ScanKeyInit](../S/ScanKeyInit.md), table_open, table_close
  - [systable_beginscan](../s/systable_beginscan.md), systable_endscan, systable_getnext
  - HeapTupleIsValid, GETSTRUCT
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md), F_OIDEQ, BTEqualStrategyNumber
  - elog, Assert
  - OpClassCacheEnt, HASHCTL, SysScanDesc, Form_pg_opclass, Form_pg_amproc (types)
- Called from:
  - [IndexSupportInitialize](../I/IndexSupportInitialize.md)

## Notes and Other Information
- Implements a persistent cache that is never flushed (acceptable since operator classes are rarely modified)
- Uses a hash table for O(1) lookup performance after initial population
- Handles bootstrap scenarios by detecting critical operator classes and using heap scans instead of index scans
- Allocates cache entries in CacheMemoryContext for persistence across transactions
- Supports cache invalidation testing through debug_discard_caches when DISCARD_CACHES_ENABLED is defined
- Scans pg_amproc to find only default support procedures (lefttype = righttype = opcintype)
- Critical for index performance as it avoids repeated catalog lookups during index operations
- The cache entries become dead but harmless if operator classes are dropped

## Simplified Source

```c
static OpClassCacheEnt *LookupOpclassInfo(Oid operatorClassOid, StrategyNumber numSupport) {
    // Initialize cache on first use
    if (OpClassCache == NULL) {
        HASHCTL ctl;
        if (!CacheMemoryContext) CreateCacheMemoryContext();

        ctl.keysize = sizeof(Oid);
        ctl.entrysize = sizeof(OpClassCacheEnt);
        OpClassCache = hash_create("Operator class cache", 64, &ctl, HASH_ELEM | HASH_BLOBS);
    }

    // Look up or create cache entry
    bool found;
    OpClassCacheEnt *opcentry = hash_search(OpClassCache, &operatorClassOid, HASH_ENTER, &found);

    if (!found) {
        // Initialize new entry
        opcentry->valid = false;
        opcentry->numSupport = numSupport;
        opcentry->supportProcs = NULL;
    }

    // Return cached entry if valid
    if (opcentry->valid) return opcentry;

    // Allocate support procedure array
    if (opcentry->supportProcs == NULL && numSupport > 0) {
        opcentry->supportProcs = MemoryContextAllocZero(CacheMemoryContext,
                                                       numSupport * sizeof(RegProcedure));
    }

    // Determine if we can use indexes (avoid bootstrap issues)
    bool indexOK = criticalRelcachesBuilt ||
                   (operatorClassOid != OID_BTREE_OPS_OID && operatorClassOid != INT2_BTREE_OPS_OID);

    // Scan pg_opclass to get family and input type
    ScanKeyData skey[3];
    ScanKeyInit(&skey[0], Anum_pg_opclass_oid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(operatorClassOid));

    Relation rel = table_open(OperatorClassRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(rel, OpclassOidIndexId, indexOK, NULL, 1, skey);

    HeapTuple htup = systable_getnext(scan);
    if (HeapTupleIsValid(htup)) {
        Form_pg_opclass opclassform = (Form_pg_opclass) GETSTRUCT(htup);
        opcentry->opcfamily = opclassform->opcfamily;
        opcentry->opcintype = opclassform->opcintype;
    }

    systable_endscan(scan);
    table_close(rel, AccessShareLock);

    // Scan pg_amproc for support procedures
    if (numSupport > 0) {
        // Set up scan keys for family and type matching
        ScanKeyInit(&skey[0], Anum_pg_amproc_amprocfamily, BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(opcentry->opcfamily));
        ScanKeyInit(&skey[1], Anum_pg_amproc_amproclefttype, BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(opcentry->opcintype));
        ScanKeyInit(&skey[2], Anum_pg_amproc_amprocrighttype, BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(opcentry->opcintype));

        rel = table_open(AccessMethodProcedureRelationId, AccessShareLock);
        scan = systable_beginscan(rel, AccessMethodProcedureIndexId, indexOK, NULL, 3, skey);

        while (HeapTupleIsValid(htup = systable_getnext(scan))) {
            Form_pg_amproc amprocform = (Form_pg_amproc) GETSTRUCT(htup);

            // Store support procedure in array
            if (amprocform->amprocnum > 0 && amprocform->amprocnum <= numSupport) {
                opcentry->supportProcs[amprocform->amprocnum - 1] = amprocform->amproc;
            }
        }

        systable_endscan(scan);
        table_close(rel, AccessShareLock);
    }

    opcentry->valid = true;
    return opcentry;
}
```