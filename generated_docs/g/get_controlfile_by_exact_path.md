# get_controlfile_by_exact_path

## Location
src/common/controldata_utils.c: 68 - 188

## Overview
The get_controlfile_by_exact_path function reads PostgreSQL's control file from a specified file path, performing CRC validation and handling concurrent write scenarios with retry logic in frontend environments.

## Definition
ControlFileData *get_controlfile_by_exact_path(const char *ControlFilePath, bool *crc_ok_p)

## Detailed Description
This function provides the core functionality for reading PostgreSQL control files from disk. It handles both backend and frontend environments differently, using OpenTransientFile/CloseTransientFile in the backend and regular open/close in frontend applications. The function includes robust error handling, CRC validation, and special retry logic for frontend applications to handle concurrent writes from the server.

Key features include:
- CRC integrity checking of the control file data
- Frontend-specific retry mechanism to handle concurrent server writes
- Byte ordering validation to detect incompatible architectures
- Comprehensive error reporting with different mechanisms for backend vs frontend

The retry logic is particularly important for frontend tools that may be reading the control file while the server is actively writing to it, which can result in partially updated reads on some systems.

## Parameters / Member Variables
- : The complete file system path to the pg_control file to be read
- : Output parameter that receives the CRC validation result (true if CRC matches, false if corrupted)

## Dependencies
- Functions called/Symbols referenced:
  - palloc_object (for memory allocation)
  - OpenTransientFile/CloseTransientFile (backend file operations)
  - open/close (frontend file operations)
  - read (system call for reading file data)
  - INIT_CRC32C, COMP_CRC32C, FIN_CRC32C, EQ_CRC32C (CRC calculation macros)
  - pg_usleep (for retry delays in frontend)
  - ereport/pg_fatal (error reporting)
- Called from (representative examples):
  - get_controlfile
  - check_control_files
  - verify_control_file

## Notes and Other Information
- Returns a palloc'd copy of control file data that must be freed by the caller
- Uses conditional compilation (#ifdef FRONTEND) to provide different behavior for backend vs frontend usage
- Frontend version includes retry logic (up to 10 attempts) to handle concurrent server writes
- Validates byte ordering by checking pg_control_version field structure
- CRC validation covers all data except the CRC field itself using offsetof(ControlFileData, crc)
- In frontend environments, short sleeps (10ms) between retries help avoid reading partially written data
- Critical for PostgreSQL startup, recovery, and various utility operations that need to examine cluster state