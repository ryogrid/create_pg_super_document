# read_string_with_null

## Location
[src/backend/utils/misc/guc.c:5717-5748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L5717-L5748)

## Overview
read_string_with_null reads a null-terminated string from a binary file, with dynamic memory allocation and error handling for malformed input.

## Definition


## Detailed Description
read_string_with_null is a static utility function that reads null-terminated strings from a binary file stream. It is specifically designed to deserialize strings written by write_one_nondefault_variable during the EXEC_BACKEND configuration sharing process.

Key features:
- **Dynamic allocation**: Starts with 256-byte buffer and doubles size as needed
- **Null termination**: Reads until it encounters a null byte (0)
- **EOF handling**: Returns NULL if EOF is encountered immediately (no data read)
- **Error detection**: Issues FATAL error if EOF occurs in the middle of reading a string
- **Memory management**: Uses PostgreSQL's GUC memory allocation functions

The function reads character by character until it finds a null terminator, dynamically growing the buffer when needed. This approach handles strings of arbitrary length while maintaining memory efficiency.

Error conditions:
- **EOF at start**: Returns NULL (normal end-of-file condition)
- **EOF mid-string**: Calls elog(FATAL) as this indicates file corruption

## Parameters / Member Variables
- : File pointer to read the null-terminated string from

## Dependencies
- Functions called/Symbols referenced:
  - fgetc
  - [guc_malloc](../g/guc_malloc.md)
  - [guc_realloc](../g/guc_realloc.md)
  - elog
- Called from (representative examples):
  - [read_nondefault_variables](read_nondefault_variables.md) (multiple calls for name, value, and sourcefile)

## Notes and Other Information
- This is a static function only used within guc.c
- Part of the EXEC_BACKEND mechanism for deserializing configuration state
- Initial buffer size is 256 bytes, doubled when capacity is exceeded
- Uses FATAL error level because configuration file corruption is a critical system error
- The function complements write_one_nondefault_variable's string serialization format
- Memory allocated must be freed by the caller using appropriate GUC memory functions
- Designed to handle the specific binary format used by PostgreSQL's configuration sharing
- Buffer growth strategy (doubling) provides good amortized performance for large strings