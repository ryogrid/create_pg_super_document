# DataChecksumsEnabled

## Location
src/backend/access/transam/xlog.c: 4543 - 4558

## Overview
Determines whether data page checksums are enabled for the current PostgreSQL database cluster.

## Definition
```c
bool DataChecksumsEnabled(void)
```

## Detailed Description
DataChecksumsEnabled is a utility function that checks if data page checksums are enabled for the current database cluster. It examines the data_checksum_version field in the control file to determine if checksums are active. Data page checksums are a data integrity feature that allows PostgreSQL to detect corruption in data pages by storing and verifying checksum values.

The function returns true if the data_checksum_version is greater than 0, indicating that checksums are enabled. A value of 0 means checksums are disabled for this cluster.

## Parameters / Member Variables
This function takes no parameters and returns a boolean value indicating checksum status.

## Dependencies
- Functions called/Symbols referenced:
  - ControlFile (global variable access)
  - Assert (assertion check)
- Called from (representative examples):
  - ReadControlFile
  - [sendFile](../s/sendFile.md)
  - PageIsVerifiedExtended
  - [PageSetChecksumCopy](../P/PageSetChecksumCopy.md)
  - [PageSetChecksumInplace](../P/PageSetChecksumInplace.md)
  - [pg_stat_get_db_checksum_failures](../p/pg_stat_get_db_checksum_failures.md)
  - XLogHintBitIsNeeded

## Notes and Other Information
- The function includes an assertion to ensure ControlFile is not NULL before accessing it
- Checksums can only be enabled during initdb and cannot be changed afterward without reinitializing the cluster
- This function is widely used throughout the storage system to conditionally enable checksum-related operations
- When checksums are enabled, PostgreSQL will verify page integrity when reading pages and compute checksums when writing pages
- The checksum version allows for future evolution of the checksum algorithm while maintaining backward compatibility
- Located in src/backend/access/transam/xlog.c:4543-4558