# PageIsVerifiedExtended

## Location
src/backend/storage/page/bufpage.c: 88 - 193

## Overview
Validates that a page header and checksum appear correct when a page is read from disk, providing protection against corrupted data before processing page contents.

## Definition


## Detailed Description
PageIsVerifiedExtended performs comprehensive validation of a PostgreSQL page that has just been read from disk. The function serves as a first line of defense against data corruption by checking the page header structure, validating checksums (if enabled), and ensuring page boundaries are sensible before allowing the page into the buffer pool. It handles special cases like all-zero pages, which can legitimately exist due to system crashes during page extension. The function supports configurable error reporting through flags and can optionally ignore checksum failures in certain scenarios.

## Parameters / Member Variables
- : Pointer to the page buffer to be verified
- : Block number of the page for checksum calculation
- : Control flags for error reporting behavior (PIV_LOG_WARNING, PIV_REPORT_STAT)

## Dependencies
- Functions called/Symbols referenced:
  - PageHeader (type cast)
  - PageIsNew (checks if page is all zeros)
  - DataChecksumsEnabled (checks if checksums are enabled)
  - pg_checksum_page (calculates page checksum)
  - pgstat_report_checksum_failure (reports checksum failures to stats)
  - PD_VALID_FLAG_BITS (valid page flag bits constant)
  - PIV_LOG_WARNING (flag for logging warnings)
  - PIV_REPORT_STAT (flag for reporting to statistics)
  - ERRCODE_DATA_CORRUPTED (error code constant)
- Called from (representative examples):
  - RelationCopyStorage (storage copy operations)
  - WaitReadBuffers (buffer manager read operations)
  - PageIsVerified (inline wrapper function)

## Notes and Other Information
- Allows zeroed pages as valid since they can occur after crashes during relation extension
- Checksum validation is only performed when DataChecksumsEnabled() returns true
- Header sanity checks include flag validation and boundary consistency (pd_lower <= pd_upper <= pd_special)
- All-zero page detection uses efficient word-aligned comparison
- The ignore_checksum_failure global variable can override checksum validation failures
- Returns true for valid pages (including all-zero pages), false for corrupted pages
- Error reporting is configurable through the flags parameter for different use cases