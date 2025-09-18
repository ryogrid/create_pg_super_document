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
- None (operates on the global DataDir variable)

## Dependencies
- Functions called/Symbols referenced:
  - [stat](../s/stat.md) (system call for file information)
  - S_ISDIR (macro to check if path is directory)
  - PG_MODE_MASK_GROUP (permission mask constant)
  - SetDataDirectoryCreatePerm (sets file creation permissions)
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