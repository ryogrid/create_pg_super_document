# verify_page_checksum

## Location
src/backend/backup/basebackup.c: 1991 - 2018

## Overview
Validates the checksum of a PostgreSQL page during base backup operations, ensuring data integrity by comparing the stored checksum with a calculated value.

## Definition
```c
static bool verify_page_checksum(Page page, XLogRecPtr start_lsn, BlockNumber blkno, uint16 *expected_checksum)
```

## Detailed Description
This function performs checksum verification for database pages during base backup operations. It implements a careful validation strategy that accounts for concurrent database modifications. The function only verifies pages that have not been modified since the base backup started, as partially written pages during backup could have invalid checksums that would be corrected by WAL replay. New pages without checksums are also skipped. When verification fails, the function provides the expected checksum value for diagnostic purposes.

## Parameters / Member Variables
- `page`: Pointer to the database page to be verified
- `start_lsn`: WAL Log Sequence Number marking the start of the base backup
- `blkno`: Block number of the page being verified (used in checksum calculation)
- `expected_checksum`: Output parameter that receives the calculated checksum when verification fails

## Dependencies
- Functions called/Symbols referenced:
  - [PageIsNew](../P/PageIsNew.md)
  - [PageGetLSN](../P/PageGetLSN.md)
  - [pg_checksum_page](../p/pg_checksum_page.md)
  - PageHeader
- Called from (representative examples):
  - [read_file_data_into_buffer](../r/read_file_data_into_buffer.md)
  - [basebackup_options](../b/basebackup_options.md)

## Notes and Other Information
- Returns true for successful verification or when verification is skipped (new pages, recently modified pages)
- Returns false only when checksum verification actually fails
- The function is static, indicating it is only used within the basebackup.c module
- Critical for ensuring data integrity during base backup operations
- Handles the edge case where pages might be partially written during backup by checking LSN values