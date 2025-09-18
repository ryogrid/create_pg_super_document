# PageGetLSN

## Location
src/include/storage/bufpage.h: 384 - 388

## Overview
Retrieves the Log Sequence Number (LSN) from a page header, providing access to the last WAL record position that modified the page for recovery and consistency purposes.

## Definition
static inline XLogRecPtr PageGetLSN(Page page)

## Detailed Description
PageGetLSN extracts the Log Sequence Number (LSN) from a page's header, which represents the position of the last Write-Ahead Logging (WAL) record that modified the page. The LSN is crucial for PostgreSQL's crash recovery mechanism and ensures data consistency by tracking the order of modifications across the entire database.

The function uses PageXLogRecPtrGet to properly extract and convert the LSN value from the page header's pd_lsn field. This LSN information is essential for determining whether a page needs to be replayed during recovery, comparing page versions, and maintaining consistency in replication scenarios.

This function is fundamental to PostgreSQL's durability guarantees and is used extensively throughout the system for recovery, backup consistency, and buffer management operations.

## Parameters / Member Variables
- : A Page pointer from which to retrieve the LSN value

## Dependencies
- Functions called/Symbols referenced:
  - [PageXLogRecPtrGet](PageXLogRecPtrGet.md) (function to properly extract XLogRecPtr from page header)
  - PageHeader (type cast for accessing page header structure)
- Called from (representative examples):
  - WAL operations (XLogRecordAssemble, XLogCheckBufferNeedsBackup, XLogReadBufferForRedoExtended)
  - Buffer management (BufferGetLSN, BufferGetLSNAtomic)
  - Recovery operations (verifyBackupPageConsistency)
  - Index operations (gistdoinsert, _bt_split, _bt_dedup_pass)
  - Vacuum operations (lazy_scan_new_or_empty)
  - Backup operations (verify_page_checksum)

## Notes and Other Information
- The LSN represents the WAL position of the last modification to the page
- Critical for crash recovery to determine which pages need replay of WAL records
- Used in backup consistency checks to ensure pages are consistent with WAL position
- Essential for replication and standby server consistency
- The function provides atomic access to the LSN value through proper conversion
- Lower LSN values indicate older modifications, higher values indicate newer ones
- Used extensively in buffer pool management for write-ordering and checkpoint operations
- Fundamental to PostgreSQL's ACID compliance and durability guarantees