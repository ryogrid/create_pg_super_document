# LookupOpclassInfo

## Location
src/backend/utils/cache/relcache.c: 1648 - 1800

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
  - hash_create, hash_search
  - CreateCacheMemoryContext, MemoryContextAllocZero
  - ScanKeyInit, table_open, table_close
  - systable_beginscan, systable_endscan, systable_getnext
  - HeapTupleIsValid, GETSTRUCT
  - ObjectIdGetDatum, F_OIDEQ, BTEqualStrategyNumber
  - elog, Assert
  - OpClassCacheEnt, HASHCTL, SysScanDesc, Form_pg_opclass, Form_pg_amproc (types)
- Called from:
  - IndexSupportInitialize

## Notes and Other Information
- Implements a persistent cache that is never flushed (acceptable since operator classes are rarely modified)
- Uses a hash table for O(1) lookup performance after initial population
- Handles bootstrap scenarios by detecting critical operator classes and using heap scans instead of index scans
- Allocates cache entries in CacheMemoryContext for persistence across transactions
- Supports cache invalidation testing through debug_discard_caches when DISCARD_CACHES_ENABLED is defined
- Scans pg_amproc to find only default support procedures (lefttype = righttype = opcintype)
- Critical for index performance as it avoids repeated catalog lookups during index operations
- The cache entries become dead but harmless if operator classes are dropped