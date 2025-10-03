# get_controlfile

## Location
[src/common/controldata_utils.c:52-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/controldata_utils.c#L52-L67)

## Overview
The get_controlfile function retrieves PostgreSQL's control file data from a specified data directory, returning a dynamically allocated copy of the control file with CRC validation status.

## Definition

```c
ControlFileData *
get_controlfile(const char *DataDir, bool *crc_ok_p)
```
## Detailed Description
This function serves as a convenience wrapper around get_controlfile_by_exact_path, constructing the standard control file path from a PostgreSQL data directory. It reads the pg_control file located at DataDir/global/pg_control and returns the control file data structure. The function provides CRC validation feedback to the caller, allowing them to determine whether the control file data integrity is intact.

The control file contains critical cluster metadata including system identifier, database state, checkpoint information, and configuration parameters essential for PostgreSQL startup and recovery operations.

## Parameters / Member Variables
- `*DataDir`: The PostgreSQL data directory path where the control file should be located
- `*crc_ok_p`: Output parameter that receives the CRC validation result (true if CRC is valid, false otherwise)
## Dependencies
- Functions called/Symbols referenced:
  - [get_controlfile_by_exact_path](get_controlfile_by_exact_path.md)
  - snprintf
- Called from (representative examples):
  - [pg_control_system](../p/pg_control_system.md)
  - [pg_control_checkpoint](../p/pg_control_checkpoint.md) 
  - [pg_control_recovery](../p/pg_control_recovery.md)
  - [pg_control_init](../p/pg_control_init.md)
  - [get_standby_sysid](get_standby_sysid.md)
  - [main](../m/main.md) (in pg_checksums, pg_controldata)
  - [get_control_dbstate](get_control_dbstate.md)

## Notes and Other Information
- Returns a palloc'd copy of control file data that must be freed by the caller
- Constructs the standard control file path using MAXPGPATH buffer size
- Delegates actual file reading and validation to get_controlfile_by_exact_path
- Used extensively by PostgreSQL utilities and backend functions that need to examine cluster state
- The CRC check is crucial for detecting control file corruption which could indicate serious cluster problems