# RelationInitPhysicalAddr

## Location
[src/backend/utils/cache/relcache.c:1320-1401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L1320-L1401)

## Overview
Initializes the physical addressing information (RelFileLocator) for a relation cache entry, determining the tablespace, database, and file number for the relation's storage.

## Definition


## Detailed Description
This function sets up the RelFileLocator structure within a relation descriptor to establish the physical location of the relation's data files on disk. It handles different scenarios including normal relations with explicit file nodes, mapped relations that require consultation with the relation mapper, and special cases for global tablespace relations.

The function includes special handling for logical decoding scenarios where historic snapshots are active, ensuring that the file node points to the current file even when using older catalog snapshots. It also handles parallel worker considerations for WAL logging decisions.

## Parameters / Member Variables
- : The relation descriptor whose physical addressing information needs to be initialized

## Dependencies
- Functions called/Symbols referenced:
  - RELKIND_HAS_STORAGE (macro to check if relation kind has storage)
  - HistoricSnapshotActive (check if using historic snapshot)
  - RelationIsAccessibleInLogicalDecoding (check logical decoding accessibility)
  - [IsTransactionState](../I/IsTransactionState.md) (check transaction state)
  - [ScanPgRelation](../S/ScanPgRelation.md) (scan pg_class for current tuple)
  - [RelationMapOidToFilenumber](RelationMapOidToFilenumber.md) (map OID to file number)
  - RelFileNumberIsValid (validate file number)
  - IsParallelWorker (check if in parallel worker)
  - [RelFileLocatorSkippingWAL](RelFileLocatorSkippingWAL.md) (check WAL skipping status)
- Called from (representative examples):
  - [RelationBuildDesc](RelationBuildDesc.md)
  - [formrdesc](../f/formrdesc.md)
  - [RelationReloadIndexInfo](RelationReloadIndexInfo.md)
  - [RelationBuildLocalRelation](RelationBuildLocalRelation.md)

## Notes and Other Information
- Relations in pg_global tablespace are treated as shared regardless of relisshared flag
- Returns early for relation kinds that never have storage (views, composite types, etc.)
- Sets spcOid to relation's tablespace or MyDatabaseTableSpace if none specified
- For global tablespace relations, dbOid is set to InvalidOid, otherwise MyDatabaseId
- Handles mapped relations (like system catalogs) using RelationMapOidToFilenumber
- Special logic for logical decoding ensures current file nodes are used even with historic snapshots
- Parallel worker support includes proper rd_firstRelfilelocatorSubid setup for WAL decisions
- Critical for establishing the connection between logical relation identifiers and physical storage files