# pg_checksum_page

## Location
src/include/storage/checksum_impl.h: 187 - 215

## Overview
Computes a 16-bit checksum for a PostgreSQL page, incorporating the block number for transposition detection and temporarily zeroing the existing checksum field during calculation.

## Definition
uint16 pg_checksum_page(char *page, BlockNumber blkno)

## Detailed Description
This function provides the high-level interface for computing page checksums in PostgreSQL. It safely handles the existing checksum field by temporarily zeroing it during calculation to avoid interference, then restores the original value since updating the checksum is not part of this function's responsibility. The function incorporates the block number into the checksum calculation to detect cases where pages are moved to different locations in the database. The final result is reduced to a 16-bit value with an offset of one to avoid zero checksums.

The algorithm includes several important safety measures:
- Validates that the page is properly initialized (not a new/empty page)
- Temporarily clears the existing pd_checksum field to prevent interference
- Mixes in the block number for location verification
- Reduces the 32-bit result to 16-bit with offset to avoid zero values

## Parameters / Member Variables
- : A character pointer to the page data to be checksummed, cast internally to PGChecksummablePage
- : The block number of the page, used for transposition detection by mixing into the final checksum

## Dependencies
- Functions called/Symbols referenced:
  - PGChecksummablePage (data structure for page representation)
  - PageIsNew (function to check if page is initialized)
  - pg_checksum_block (core checksum computation function)
- Called from (representative examples):
  - verify_page_checksum (in basebackup.c)
  - PageIsVerifiedExtended (in bufpage.c)
  - PageSetChecksumCopy (in bufpage.c)
  - PageSetChecksumInplace (in bufpage.c)
  - scan_file (in pg_checksums.c)
  - rewriteVisibilityMap (in pg_upgrade)

## Notes and Other Information
- The function requires pages to be adequately aligned (at least 4-byte boundary)
- Temporarily modifies the pd_checksum field but restores it before returning
- Returns values in range 1-65535 (never zero) by using modulo 65535 plus 1
- Used throughout PostgreSQL for page integrity verification and corruption detection
- Critical for data reliability in storage systems and backup/restore operations