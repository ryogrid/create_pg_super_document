# isValidTarHeader

## Location
src/bin/pg_dump/pg_backup_tar.c: 988 - 1013

## Overview
A function that validates TAR archive headers by checking the checksum and magic number to determine if a given header block represents a valid TAR format.

## Definition
```c
bool isValidTarHeader(char *header)
```

## Detailed Description
This function performs TAR header validation by first verifying the checksum integrity and then checking for recognized TAR format magic numbers. It supports multiple TAR format variants including POSIX tar, GNU tar, and a legacy format used by PostgreSQL pg_dump prior to version 9.3. The function calculates the header checksum using tarChecksum() and compares it against the stored checksum in the header. If the checksums match, it proceeds to examine the magic number field to identify the specific TAR format variant.

## Parameters / Member Variables
- `header`: Pointer to a 512-byte TAR header block to be validated

## Dependencies
- Functions called/Symbols referenced:
  - tarChecksum (calculates header checksum)
  - read_tar_number (reads numeric values from TAR header fields)
  - TAR_OFFSET_CHECKSUM (offset constant for checksum field)
  - TAR_OFFSET_MAGIC (offset constant for magic number field)
  - TAR_OFFSET_VERSION (offset constant for version field)
- Called from (representative examples):
  - _discoverArchiveFormat (archive format detection)
  - appendByteaLiteralAHX (utility function)

## Notes and Other Information
- Validates three TAR format variants: POSIX ("ustar\0" + "00"), GNU ("ustar  \0"), and pre-9.3 pg_dump ("ustar00\0")
- Returns true only if both checksum verification and format recognition succeed
- Essential for archive format auto-detection in PostgreSQL backup/restore operations
- The function assumes the header parameter points to a complete 512-byte TAR header block
- Located in src/bin/pg_dump/pg_backup_tar.c:988-1013