# check_application_name

## Location
[src/backend/commands/variable.c:1068-1095](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L1068-L1095)

## Overview
The `check_application_name` function validates and sanitizes the application_name configuration parameter, ensuring it contains only clean ASCII characters.

## Definition
```c
bool check_application_name(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function is a GUC (Grand Unified Configuration) check hook specifically designed to validate and clean the application_name parameter. The application_name is a user-visible identifier that appears in various PostgreSQL system views and log messages, so it requires careful sanitization to prevent issues with logging, monitoring, and display systems.

The function implements a multi-step sanitization process:
1. **ASCII Cleaning**: Uses pg_clean_ascii to remove or replace non-ASCII and potentially problematic characters
2. **Memory Management**: Properly handles memory allocation and cleanup for the sanitized string
3. **String Replacement**: Replaces the original string with the cleaned version
4. **Error Handling**: Returns false if memory allocation fails during the cleaning process

The cleaning process ensures that the application_name is safe for use in log files, system views, and other contexts where special characters might cause parsing issues or security concerns. This is particularly important since application names can come from untrusted sources like client connections.

## Parameters / Member Variables
- `newval`: Pointer to the application name string being validated; will be replaced with cleaned version
- `extra`: Output parameter for additional data (unused in this function, maintained for GUC hook interface)
- `source`: The source of the configuration change (unused in validation logic but required for interface)

## Dependencies
- Functions called/Symbols referenced:
  - pg_clean_ascii
  - [guc_strdup](../g/guc_strdup.md)
  - [guc_free](../g/guc_free.md)
  - [pfree](../p/pfree.md)
  - MCXT_ALLOC_NO_OOM (memory allocation flag)
- Called from (representative examples):
  - GUC system framework (as check hook for application_name parameter)

## Notes and Other Information
- Critical for security and system stability by preventing malicious or malformed application names
- Memory allocation failures are handled gracefully with proper cleanup
- Part of PostgreSQL's defense against log injection and display corruption attacks
- The cleaned string becomes the canonical application name used throughout the system
- Important for monitoring tools that parse PostgreSQL logs and system views
- Ensures consistent behavior across different client libraries and connection methods
- Applied to application names from both configuration files and runtime SET commands