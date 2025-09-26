# ValidatePgVersion

## Location
[src/backend/utils/init/miscinit.c:1765-1845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L1765-L1845)

## Overview
ValidatePgVersion verifies that the PostgreSQL data directory version is compatible with the current server version by reading and comparing the PG_VERSION file.

## Definition

```c
void
ValidatePgVersion(const char *path)
```
## Detailed Description
This function performs critical version compatibility checking during PostgreSQL startup. It reads the PG_VERSION file located in the specified data directory path and compares the major version number with the current server's major version. The function ensures that the data directory was initialized by a compatible PostgreSQL version, preventing potential data corruption or incompatibility issues that could arise from version mismatches.

The function performs several validation steps:
1. Constructs the full path to the PG_VERSION file
2. Opens and reads the version string from the file
3. Extracts the major version number from both the file and current server
4. Compares the major versions for compatibility
5. Reports fatal errors for any incompatibility or file access issues

## Parameters / Member Variables
- : The directory path where the PG_VERSION file should be located (typically the data directory)

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateFile](../A/AllocateFile.md)
  - [FreeFile](../F/FreeFile.md)
  - strtol
  - snprintf
  - fscanf
  - ereport
  - [errcode](../e/errcode.md)
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [errmsg](../e/errmsg.md)
  - [errdetail](../e/errdetail.md)
  - [errhint](../e/errhint.md)
- Called from (representative examples):
  - [checkDataDir](../c/checkDataDir.md)
  - [InitPostgres](../I/InitPostgres.md)
  - INIT_PG_OVERRIDE_ROLE_LOGIN

## Notes and Other Information
- This function always terminates with ereport(FATAL) if any validation fails, making it a critical safety check
- Only compares major version numbers, allowing minor version differences
- Provides detailed error messages to help administrators diagnose version mismatch issues
- The PG_VERSION file is a simple text file containing the PostgreSQL version string
- Used during both normal startup and various administrative operations that require data directory access