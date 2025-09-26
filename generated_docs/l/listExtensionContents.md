# listExtensionContents

## Location
[src/bin/psql/describe.c:6053-6119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L6053-L6119)

## Overview
Implements the  command in psql to list the detailed contents of installed PostgreSQL extensions by iterating through matching extensions and displaying their contents.

## Definition

```c
bool
listExtensionContents(const char *pattern)
```
## Detailed Description
This function serves as a coordinator for displaying detailed extension contents. It first queries the pg_extension catalog to find extensions matching the given pattern, then iterates through each found extension and calls listOneExtensionContents() to display the detailed contents of each extension. The function handles error cases such as no matching extensions found and provides appropriate user feedback.

The function workflow:
1. Query pg_extension for matching extensions (name and OID)
2. Validate the pattern and handle empty results
3. For each found extension, call listOneExtensionContents() to display detailed contents
4. Handle cancellation and error propagation

## Parameters / Member Variables
- : SQL name pattern for filtering extensions (can be NULL for all extensions)

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (data structure)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [listOneExtensionContents](listOneExtensionContents.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in src/bin/psql/command.c:1012)

## Notes and Other Information
- This function is part of psql's describe commands (\d family), specifically the verbose version of \dx
- Implements proper error handling and user feedback for cases where no extensions are found
- Supports cancellation via cancel_pressed global variable
- Acts as a coordinator function that delegates the actual content listing to listOneExtensionContents()
- Pattern validation is handled by validateSQLNamePattern to ensure SQL injection safety
- Returns false on any error condition, including cancellation or failure in nested function calls
- Uses pg_log_error for user-friendly error messages when extensions are not found