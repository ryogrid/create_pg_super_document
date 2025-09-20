# checkControlFile

## Location
[src/backend/postmaster/postmaster.c:1489-1517](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L1489-L1517)

## Overview
checkControlFile performs a basic existence check for the pg_control file in the data directory to verify that the directory contains a valid PostgreSQL database cluster.

## Definition

```c
static void
checkControlFile(void)
```
## Detailed Description
checkControlFile implements a fundamental sanity check during postmaster startup to ensure the specified data directory actually contains a PostgreSQL database cluster. The function performs a minimal validation by checking only for the existence and readability of the pg_control file:

1. **Path Construction**: Constructs the full path to pg_control by appending "/global/pg_control" to the DataDir global variable. The pg_control file is always located in the global subdirectory of a PostgreSQL data directory.

2. **File Access Test**: Attempts to open the pg_control file in binary read mode using AllocateFile(). This verifies both the file's existence and the process's ability to read it.

3. **Error Handling**: If the file cannot be opened, the function writes a detailed error message to stderr explaining the expected location and immediately terminates the postmaster with exit code 2.

4. **Cleanup**: If the file opens successfully, it is immediately closed with FreeFile() since this is only an existence check, not a content validation.

This check serves as an early gate to prevent the postmaster from attempting to start with an invalid or non-existent data directory, providing clear diagnostic information to help users identify configuration problems.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - AllocateFile: Open file for reading with PostgreSQL's file management
  - FreeFile: Close file opened with AllocateFile
  - [write_stderr](../w/write_stderr.md): Output error message to standard error
  - [ExitPostmaster](../E/ExitPostmaster.md): Terminate postmaster process with specified exit code
  - PG_BINARY_R: Binary read mode constant for file operations
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md): Called during startup sequence at line 815
  - [digestControlFile](../d/digestControlFile.md) (in pg_rewind): Used for control file validation at line 1046
  - Referenced in SIGKILL_CHILDREN_AFTER_SECS context

## Notes and Other Information
- This is explicitly a sanity check only - no attempt is made to validate pg_control content, version, or CRC
- More thorough pg_control validation occurs later via LocalProcessControlFile()
- The function assumes DataDir global variable has been properly set by earlier initialization
- Uses MAXPGPATH for path buffer sizing to handle long directory names safely
- Exit code 2 specifically indicates data directory problems, distinguishing from other startup failures
- Essential for preventing cryptic failures later in startup when other components expect a valid database cluster