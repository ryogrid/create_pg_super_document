# GetCatalogSnapshot

## Location
src/backend/utils/time/snapmgr.c: 352 - 373

## Overview
Provides a snapshot that is sufficiently up-to-date for scanning system catalog relations, with special handling for logical decoding scenarios.

## Definition


## Detailed Description
GetCatalogSnapshot is a core function in PostgreSQL's snapshot management system that returns an appropriate snapshot for system catalog scans. The function implements a key optimization for logical decoding by checking if a historic snapshot is currently active. When logical decoding is in progress (detected via HistoricSnapshotActive()), it returns the HistoricSnapshot to ensure catalog visibility is consistent with the point-in-time being decoded. Otherwise, it delegates to GetNonHistoricCatalogSnapshot() for normal catalog access patterns.

This dual behavior is essential for logical decoding functionality, as it allows the decoder to see the catalog state as it existed at the time of the changes being replayed, rather than the current state.

## Parameters / Member Variables
- : OID of the system catalog relation being scanned

## Dependencies
- Functions called/Symbols referenced:
  - HistoricSnapshotActive
  - [GetNonHistoricCatalogSnapshot](GetNonHistoricCatalogSnapshot.md)
  - HistoricSnapshot
- Called from (representative examples):
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_recheck_tuple](../s/systable_recheck_tuple.md)
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md)
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)
  - [process_settings](../p/process_settings.md)

## Notes and Other Information
- Critical for logical decoding: ensures catalog scans see appropriate historical state
- System caches must be reset after logical decoding completes due to historic snapshot usage
- Located in src/backend/utils/time/snapmgr.c:352-373
- Part of PostgreSQL's MVCC (Multi-Version Concurrency Control) snapshot management system