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

## Simplified Source

```c
// Simplified version of ValidatePgVersion
void ValidatePgVersion(const char *path) {
    char full_path[MAXPGPATH];
    FILE *file;
    long file_major, my_major;
    char file_version_string[64];
    const char *my_version_string = PG_VERSION;

    // Extract major version from current server version
    my_major = strtol(my_version_string, NULL, 10);

    // Build path to PG_VERSION file
    snprintf(full_path, sizeof(full_path), "%s/PG_VERSION", path);

    // Open and read the version file
    file = AllocateFile(full_path, "r");
    if (!file) {
        // Report appropriate error based on failure type
        if (errno == ENOENT)
            ereport(FATAL, "Data directory invalid - PG_VERSION missing");
        else
            ereport(FATAL, "Could not open PG_VERSION file");
    }

    // Read version string from file
    if (fscanf(file, "%63s", file_version_string) != 1) {
        FreeFile(file);
        ereport(FATAL, "PG_VERSION file contains invalid data");
    }

    FreeFile(file);

    // Extract major version from file
    file_major = strtol(file_version_string, NULL, 10);

    // Compare major versions for compatibility
    if (my_major != file_major) {
        ereport(FATAL, "Database version incompatible with server version");
    }
}
```

Key simplifications made:
- Removed detailed error message formatting for clarity
- Consolidated error handling paths
- Simplified variable declarations and initialization
- Focused on the main logic flow: read version, parse major version, compare
- Removed complex error reporting details while preserving the essential validation logic
- Abstracted the specific error codes and detailed messages