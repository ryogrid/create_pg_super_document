# ScanPgRelation

## Location
src/backend/utils/cache/relcache.c: 339 - 408

## Overview
ScanPgRelation scans the pg_class system catalog to retrieve a specific relation tuple by OID, handling snapshot selection and tuple copying for relcache operations.

## Definition
static HeapTuple ScanPgRelation(Oid targetRelId, bool indexOK, bool force_non_historic)

## Detailed Description
ScanPgRelation is a core function used by RelationBuildDesc to locate and retrieve pg_class tuples during relation cache construction. The function performs a system catalog scan of pg_class using the target relation OID, with careful handling of snapshot selection to ensure consistency during concurrent operations. It includes safety checks for database selection and supports both index and heap scans depending on system state and caller requirements.

The function is designed to handle the complexities of concurrent updates by requiring the caller to hold at least AccessShareLock on the target relation. It returns a copied heap tuple that must be freed by the caller, ensuring the tuple remains valid after the scan completes.

## Parameters / Member Variables
- : The OID of the relation to search for in pg_class
- : Boolean flag indicating whether index scans are allowed (false forces heap scan)
- : Boolean flag to force use of a non-historic catalog snapshot for newer tuple versions

## Dependencies
- Functions called/Symbols referenced:
  - SysScanDesc (system scan descriptor type)
  - GetNonHistoricCatalogSnapshot (for non-historic snapshot acquisition)
  - systable_beginscan (to initiate system table scan)
  - systable_getnext (to retrieve next tuple from scan)
  - heap_copytuple (to create a copy of the retrieved tuple)
- Called from (representative examples):
  - RelationBuildDesc (main relcache building function)
  - RelationInitPhysicalAddr (for physical address initialization)
  - RelationReloadIndexInfo (for index information reloading)
  - RelationReloadNailed (for reloading nailed relations)

## Notes and Other Information
- The function includes a critical safety check preventing pg_class access before database selection
- Uses AccessShareLock on pg_class during the scan operation
- Supports both index and heap scans based on criticalRelcachesBuilt state and indexOK parameter
- The returned tuple is a palloc'd copy that must be freed with heap_freetuple
- Snapshot selection logic accommodates both normal and non-historic catalog access patterns
- Essential for maintaining relcache consistency during concurrent database operations