# get_controlfile_by_exact_path

## Location
[src/common/controldata_utils.c:68-188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/controldata_utils.c#L68-L188)

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
  - [OpenTransientFile](../O/OpenTransientFile.md)/CloseTransientFile (backend file operations)
  - open/close (frontend file operations)
  - read (system call for reading file data)
  - INIT_CRC32C, COMP_CRC32C, FIN_CRC32C, EQ_CRC32C (CRC calculation macros)
  - [pg_usleep](../p/pg_usleep.md) (for retry delays in frontend)
  - ereport/pg_fatal (error reporting)
- Called from (representative examples):
  - [get_controlfile](get_controlfile.md)
  - [check_control_files](../c/check_control_files.md)
  - [verify_control_file](../v/verify_control_file.md)

## Notes and Other Information
- Returns a palloc'd copy of control file data that must be freed by the caller
- Uses conditional compilation (#ifdef FRONTEND) to provide different behavior for backend vs frontend usage
- Frontend version includes retry logic (up to 10 attempts) to handle concurrent server writes
- Validates byte ordering by checking pg_control_version field structure
- CRC validation covers all data except the CRC field itself using offsetof(ControlFileData, crc)
- In frontend environments, short sleeps (10ms) between retries help avoid reading partially written data
- Critical for PostgreSQL startup, recovery, and various utility operations that need to examine cluster state

## Simplified Source

```c
ControlFileData *
get_controlfile_by_exact_path(const char *ControlFilePath, bool *crc_ok_p)
{
    ControlFileData *ControlFile;
    int fd;
    pg_crc32c calculated_crc;
    int bytes_read;

    ControlFile = palloc_object(ControlFileData);

    // Open control file (different methods for backend vs frontend)
#ifndef FRONTEND
    fd = OpenTransientFile(ControlFilePath, O_RDONLY | PG_BINARY);
    if (fd == -1)
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not open file \"%s\" for reading: %m", ControlFilePath)));
#else
    fd = open(ControlFilePath, O_RDONLY | PG_BINARY, 0);
    if (fd == -1)
        pg_fatal("could not open file \"%s\" for reading: %m", ControlFilePath);
#endif

    // Read the complete control file data
    bytes_read = read(fd, ControlFile, sizeof(ControlFileData));
    if (bytes_read != sizeof(ControlFileData)) {
        // Handle read errors (abbreviated for simplicity)
        pg_fatal("could not read control file completely");
    }

    // Close file
#ifndef FRONTEND
    CloseTransientFile(fd);
#else
    close(fd);
#endif

    // Verify CRC integrity
    INIT_CRC32C(calculated_crc);
    COMP_CRC32C(calculated_crc, (char *) ControlFile, offsetof(ControlFileData, crc));
    FIN_CRC32C(calculated_crc);
    *crc_ok_p = EQ_CRC32C(calculated_crc, ControlFile->crc);

    // Validate byte ordering
    if (ControlFile->pg_control_version % 65536 == 0 &&
        ControlFile->pg_control_version / 65536 != 0) {
        pg_log_warning("possible byte ordering mismatch");
    }

    return ControlFile;
}
```