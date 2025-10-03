# checkDataDir

## Location
[src/backend/utils/init/miscinit.c:342-434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L342-L434)

## Overview
Validates the PostgreSQL data directory for security, ownership, permissions, and proper configuration before allowing the server to start.

## Definition
```c
void checkDataDir(void)
```

## Detailed Description
checkDataDir performs comprehensive validation of the PostgreSQL data directory to ensure it meets security and operational requirements. The function verifies directory existence, ownership, permissions, and proper PostgreSQL version compatibility. It also configures file creation modes based on the data directory's permissions. This function is a critical security component that prevents unauthorized access and ensures proper multi-user isolation. The validation includes platform-specific checks, with Windows having relaxed permission requirements due to different file system security models.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [stat](../s/stat.md) (system call for file information)
  - S_ISDIR (macro to check if path is directory)
  - PG_MODE_MASK_GROUP (permission mask constant)
  - [SetDataDirectoryCreatePerm](../S/SetDataDirectoryCreatePerm.md) (sets file creation permissions)
  - [ValidatePgVersion](../V/ValidatePgVersion.md) (verifies PostgreSQL version compatibility)
  - ereport, errcode_for_file_access, errmsg (error reporting functions)
- Called from (representative examples):
  - [BootstrapModeMain](../B/BootstrapModeMain.md)
  - [SubPostmasterMain](../S/SubPostmasterMain.md)
  - [PostmasterMain](../P/PostmasterMain.md)
  - [PostgresSingleUserMain](../P/PostgresSingleUserMain.md)
  - AmSpecialWorkerProcess

## Notes and Other Information
This function implements several important security measures: (1) Ownership verification prevents unauthorized users from starting PostgreSQL on someone else's data, (2) Permission validation ensures data directory has secure modes (0700 or 0750), (3) PG_VERSION validation prevents version mismatches that could corrupt data. The function is part of the postmaster interlock mechanism that prevents multiple PostgreSQL instances from operating on the same data directory simultaneously. Platform-specific conditional compilation accounts for Windows' different security model.

## Simplified Source

```c
// Simplified version of checkDataDir
void checkDataDir(void) {
    struct stat stat_buf;

    // Step 1: Verify data directory exists and is accessible
    if (stat(DataDir, &stat_buf) != 0) {
        if (errno == ENOENT) {
            ereport(FATAL, "data directory does not exist");
        } else {
            ereport(FATAL, "could not read directory permissions");
        }
    }

    // Step 2: Ensure it's actually a directory
    if (!S_ISDIR(stat_buf.st_mode)) {
        ereport(FATAL, "specified path is not a directory");
    }

    // Step 3: Verify ownership (Unix/Linux only)
    #if !defined(WIN32) && !defined(__CYGWIN__)
    if (stat_buf.st_uid != geteuid()) {
        ereport(FATAL, "data directory has wrong ownership");
    }
    #endif

    // Step 4: Check directory permissions (Unix/Linux only)
    #if !defined(WIN32) && !defined(__CYGWIN__)
    if (stat_buf.st_mode & PG_MODE_MASK_GROUP) {
        ereport(FATAL, "data directory has invalid permissions");
    }
    #endif

    // Step 5: Set file creation permissions based on directory mode
    #if !defined(WIN32) && !defined(__CYGWIN__)
    SetDataDirectoryCreatePerm(stat_buf.st_mode);
    umask(pg_mode_mask);
    data_directory_mode = pg_dir_create_mode;
    #endif

    // Step 6: Validate PostgreSQL version compatibility
    ValidatePgVersion(DataDir);
}
```

Key simplifications made:
- Removed detailed error message formatting for clarity
- Consolidated platform-specific checks into clear sections
- Abstracted complex permission validation logic
- Simplified error handling to focus on main cases
- Added step-by-step comments for the validation flow
- Focused on the main execution path without detailed error context