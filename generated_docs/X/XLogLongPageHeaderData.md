# XLogLongPageHeaderData

## Location
src/include/access/xlog_internal.h: 61 - 67

## Overview
A data structure representing the extended header used for XLOG pages when the XLP_LONG_HEADER flag is set, typically in the first page of an XLOG file to accurately identify the file.

## Definition


## Detailed Description
XLogLongPageHeaderData extends the standard XLogPageHeaderData with additional fields used for file identification and validation. This extended header is primarily used in the first page of XLOG files when the XLP_LONG_HEADER flag is set. The additional fields provide system identification information and cross-check values to ensure file integrity and proper file identification across different PostgreSQL instances.

The structure serves as a safeguard mechanism to prevent XLOG files from being incorrectly used across different database clusters or configurations, as the system identifier and block size information must match the current system's configuration.

## Parameters / Member Variables
- : Standard XLOG page header containing magic value, info flags, timeline ID, page address, and remaining length information
- : System identifier copied from pg_control file, used to ensure the XLOG file belongs to the correct database cluster
- : Cross-check value for XLOG segment size to validate file compatibility
- : Cross-check value for XLOG block size to ensure proper alignment and compatibility

## Dependencies
- Functions called/Symbols referenced:
  - XLogPageHeaderData (embedded as  member)
- Called from (representative examples):
  - SizeOfXLogLongPHD (macro for size calculation)
  - XLogLongPageHeader (type alias)

## Notes and Other Information
- This structure is only used when the XLP_LONG_HEADER flag is set in the page info field
- Typically appears in the first page of XLOG files for identification purposes
- The additional fields serve as integrity checks to prevent mismatched XLOG files from being used
- Size can be calculated using the SizeOfXLogLongPHD macro
- Part of PostgreSQL's Write-Ahead Logging (WAL) system infrastructure