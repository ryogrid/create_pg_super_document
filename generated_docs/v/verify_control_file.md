# verify_control_file

## Location
[src/bin/pg_verifybackup/pg_verifybackup.c:758-790](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_verifybackup/pg_verifybackup.c#L758-L790)

## Overview
Performs specialized validation of the PostgreSQL control file by checking its CRC, version compatibility, and verifying that its system identifier matches the manifest's system identifier.

## Definition
```c
static void verify_control_file(const char *controlpath, uint64 manifest_system_identifier)
```

## Detailed Description
This function implements critical validation logic specifically for PostgreSQL's control file (pg_control), which contains essential metadata about the database cluster. The function reads the control file using PostgreSQL's built-in utilities, validates the file's CRC to ensure data integrity, checks that the control file version matches the expected PG_CONTROL_VERSION constant, and performs a crucial system identifier comparison. The system identifier is a unique value that distinguishes one PostgreSQL cluster from another, and ensuring it matches between the manifest and actual control file is essential for backup integrity. All validation failures are treated as fatal errors since control file corruption or mismatches indicate serious backup problems.

## Parameters / Member Variables
- `controlpath`: File system path to the pg_control file being verified
- `manifest_system_identifier`: Expected system identifier from the backup manifest (uint64)

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_debug
  - [get_controlfile_by_exact_path](../g/get_controlfile_by_exact_path.md)
  - [report_fatal_error](../r/report_fatal_error.md)
  - [pfree](../p/pfree.md)
- Constants referenced:
  - PG_CONTROL_VERSION
- Types referenced:
  - [ControlFileData](../C/ControlFileData.md)
- Called from (representative examples):
  - [verify_backup_file](verify_backup_file.md)

## Notes and Other Information
- All errors in this function are fatal since control file problems indicate severe backup corruption
- The CRC validation ensures the control file hasn't been corrupted during backup or storage
- System identifier matching is crucial for ensuring the backup belongs to the intended PostgreSQL cluster
- Version checking prevents interpretation of control files from incompatible PostgreSQL versions
- Memory management includes proper cleanup with pfree for the control file data structure
- This function is only called for manifest version 2 and higher, as version 1 doesn't include system identifiers
- Part of the specialized validation pipeline for critical PostgreSQL system files