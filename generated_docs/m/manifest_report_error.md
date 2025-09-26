# manifest_report_error

## Location
[src/backend/backup/basebackup_incremental.c:1013-1039](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_incremental.c#L1013-L1039)

## Overview
A callback function invoked when an error occurs during backup manifest parsing to format and report the error using PostgreSQL's error reporting system.

## Definition
```c
static void
manifest_report_error(JsonManifestParseContext *context, const char *fmt,...)
```

## Detailed Description
This function serves as an error reporting callback during backup manifest parsing operations. It accepts a printf-style format string and variable arguments to construct detailed error messages. The function uses PostgreSQL's StringInfo infrastructure to dynamically build the error message, handling cases where the initial buffer may be too small by enlarging it as needed. Once the message is fully constructed, it reports the error using PostgreSQL's ereport system with ERROR severity, causing the current operation to abort.

## Parameters / Member Variables
- `context`: JsonManifestParseContext pointer containing parsing state (unused in current implementation)
- `fmt`: Printf-style format string for the error message
- `...`: Variable arguments corresponding to format specifiers in fmt

## Dependencies
- Functions called/Symbols referenced:
  - [initStringInfo](../i/initStringInfo.md)
  - [appendStringInfoVA](../a/appendStringInfoVA.md)
  - [enlargeStringInfo](../e/enlargeStringInfo.md)
  - ereport
  - [errmsg_internal](../e/errmsg_internal.md)
- Called from (representative examples):
  - Manifest parsing infrastructure (as error callback)

## Notes and Other Information
- This is a static function local to basebackup_incremental.c
- Uses variadic arguments (va_list) to handle flexible error message formatting
- Implements a retry loop to handle StringInfo buffer expansion when needed
- Reports errors as internal errors using errmsg_internal rather than user-facing messages
- The context parameter is currently unused but maintained for callback interface consistency
- Part of the incremental backup error handling infrastructure