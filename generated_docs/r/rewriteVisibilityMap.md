# rewriteVisibilityMap

## Location
[src/bin/pg_upgrade/file.c:216-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/file.c#L216-L359)

## Overview
Converts old-format visibility map files to the new format during PostgreSQL upgrades, transforming single-bit-per-page visibility maps to two-bit-per-page format while preserving existing visibility information.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
The rewriteVisibilityMap function performs a critical transformation during PostgreSQL upgrades when migrating from versions prior to catversion 201603011 (PostgreSQL 9.6) to newer versions. In older PostgreSQL versions, visibility maps used one bit per heap page to track all-visible pages. Modern PostgreSQL uses two bits per page: one for all-visible and one for all-frozen status.

The function reads the old single-bit visibility map file page by page and converts each page into potentially two new-format pages. For each old byte (representing 8 heap pages), it creates new 16-bit values where the old all-visible bit is preserved in the all-visible position of the new format. The all-frozen bits are left unset, allowing future VACUUM operations to set them appropriately.

The conversion process maintains the original page headers and handles edge cases like partial pages and empty trailing sections. If checksums are enabled in the new cluster, appropriate checksums are calculated for the new visibility map pages.

## Parameters / Member Variables
- : Path to the source old-format visibility map file
- : Path to the destination new-format visibility map file
- : SQL schema name of the relation (used only for error reporting)
- : SQL relation name (used only for error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - open
  - fstat
  - read
  - write
  - close
  - memcpy
  - [pg_checksum_page](../p/pg_checksum_page.md)
  - [pg_fatal](../p/pg_fatal.md)
  - PGIOAlignedBlock
  - [PageHeaderData](../P/PageHeaderData.md)
  - PageHeader
  - SizeOfPageHeaderData
  - BLCKSZ
  - BITS_PER_BYTE
  - BITS_PER_HEAPBLOCK
  - VISIBILITYMAP_ALL_VISIBLE
  - PG_BINARY
  - pg_file_create_mode
- Called from (representative examples):
  - [transfer_relfile](../t/transfer_relfile.md)

## Notes and Other Information
- Only needed when upgrading from PostgreSQL versions before 9.6 that used single-bit visibility maps
- Converts one old visibility map page into potentially two new pages due to the expanded bit format
- Preserves all-visible information but does not set all-frozen bits (left for future VACUUM)
- Handles partial pages and empty trailing sections to avoid unnecessary page expansion
- Calculates checksums for new pages if data checksums are enabled in the target cluster
- The conversion maintains the benefit of existing visibility information, avoiding the need to re-scan entire tables
- Uses aligned I/O buffers (PGIOAlignedBlock) for optimal performance
- Each old byte (8 heap pages) becomes 16 bits (8 × 2 bits per page) in the new format
- Critical for maintaining performance during major version upgrades by preserving visibility optimization data