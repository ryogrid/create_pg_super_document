# errcode_for_file_access

## Location
src/backend/utils/error/elog.c: 880 - 952

## Overview
Maps system errno values to appropriate SQL state error codes specifically for file access operations in PostgreSQL's error reporting system.

## Definition


## Detailed Description
This function automatically sets the SQLSTATE error code for the current error based on the saved errno value from a failed file operation. It maps common file system error conditions to PostgreSQL-specific SQL state codes, providing standardized error reporting across the database system. The function assumes the failing operation was some type of disk file access and categorizes errors into logical groups like permission issues, resource problems, and hardware failures.

## Parameters / Member Variables
- No parameters (void function)
- Return value: Always returns 0 (return value is not meaningful)

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type for error information)
  - CHECK_STACK_DEPTH (macro for stack depth validation)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- The primary error message string should generally include %m when this function is used
- Maps errno values to SQLSTATE codes:
  - EPERM, EACCES, EROFS → ERRCODE_INSUFFICIENT_PRIVILEGE
  - ENOENT → ERRCODE_UNDEFINED_FILE
  - EEXIST → ERRCODE_DUPLICATE_FILE
  - ENOTDIR, EISDIR, ENOTEMPTY → ERRCODE_WRONG_OBJECT_TYPE
  - ENOSPC → ERRCODE_DISK_FULL
  - ENOMEM → ERRCODE_OUT_OF_MEMORY
  - ENFILE, EMFILE → ERRCODE_INSUFFICIENT_RESOURCES
  - EIO → ERRCODE_IO_ERROR
  - All other errors → ERRCODE_INTERNAL_ERROR
- Does not increment recursion depth counter
- Located in src/backend/utils/error/elog.c:880-952