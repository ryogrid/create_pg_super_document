# ecpg_find_desc

## Location
src/interfaces/ecpg/ecpglib/descriptor.c: 832 - 846

## Overview
Finds and returns a SQL descriptor by name within the current connection context for ECPG (Embedded SQL for C).

## Definition
```c
struct descriptor *ecpg_find_desc(int line, const char *name)
```

## Detailed Description
The `ecpg_find_desc` function searches through the thread-local list of SQL descriptors to find one with a matching name. Descriptors are data structures used in ECPG to manage dynamic SQL statements and their associated metadata, including parameter information and result set descriptions.

The function performs a linear search through the linked list of descriptors maintained per connection/thread. If no matching descriptor is found, it raises an ECPG error with the specific error code `ECPG_UNKNOWN_DESCRIPTOR` and SQL state `ECPG_SQLSTATE_INVALID_SQL_DESCRIPTOR_NAME`.

This function is essential for ECPG dynamic SQL operations where descriptors are referenced by name in SQL statements like PREPARE, EXECUTE, DESCRIBE, and FETCH operations.

## Parameters / Member Variables
- `line`: Line number in the source code where this function is called, used for error reporting and debugging purposes
- `name`: The name of the descriptor to search for (null-terminated C string)

## Dependencies
- Functions called/Symbols referenced:
  - `get_descriptors`: Retrieves the thread-local descriptor list head
  - `strcmp`: Standard C library function for string comparison
  - `ecpg_raise`: ECPG error reporting function
  - `ECPG_UNKNOWN_DESCRIPTOR`: Error code constant for unknown descriptor
  - `ECPG_SQLSTATE_INVALID_SQL_DESCRIPTOR_NAME`: SQL state constant for invalid descriptor name
- Called from (representative examples):
  - `ecpg_result_by_descriptor`: Retrieves result data using a descriptor
  - `ECPGset_desc_header`: Sets descriptor header information
  - `ECPGset_desc`: Sets descriptor item values
  - `ECPGdescribe`: Describes a prepared statement using a descriptor
  - `ecpg_build_params`: Builds parameter list from descriptor
  - `ecpg_process_output`: Processes output using descriptor information

## Notes and Other Information
- The function maintains thread-safety by using thread-local storage for descriptor lists via `get_descriptors()`
- Returns `NULL` after raising an error if the descriptor is not found, following ECPG error handling conventions
- The descriptor structure contains metadata about SQL statements including result sets, parameter information, and data types
- Part of the ECPG dynamic SQL interface that allows C programs to execute SQL statements prepared at runtime
- Error reporting includes both the line number for source-level debugging and the descriptor name for user-level diagnostics