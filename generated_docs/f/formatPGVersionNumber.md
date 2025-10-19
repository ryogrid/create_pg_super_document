# formatPGVersionNumber

## Location
[src/fe_utils/string_utils.c:313-350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L313-L350)

## Overview
Formats a PostgreSQL version number from its internal integer representation into a human-readable string, handling both modern two-part and legacy three-part version numbering schemes.

## Definition
```c
char *formatPGVersionNumber(int version_number, bool include_minor, char *buf, size_t buflen)
```

## Detailed Description
The `formatPGVersionNumber` function converts PostgreSQL version numbers from the internal PG_VERSION_NUM integer format (as returned by `PQserverVersion()`) into readable string representations. The function intelligently handles the transition between PostgreSQL's old three-part versioning scheme (e.g., "9.6.1") and the new two-part scheme (e.g., "12.1") introduced with PostgreSQL 10.

For version numbers >= 100000 (PostgreSQL 10+), it uses the new two-part format where the major version is `version_number / 10000` and the minor version is `version_number % 10000`. For older versions, it uses the three-part format where the major version is `version_number / 10000`, the minor version is `(version_number / 100) % 100`, and the patch level is `version_number % 100`.

The function is reentrant as it requires the caller to provide the output buffer, avoiding shared state issues in multi-threaded environments.

## Parameters / Member Variables
- `version_number`: Integer version number in PG_VERSION_NUM format
- `include_minor`: Boolean flag to include minor/patch version components
- `buf`: Output buffer to store the formatted version string (provided by caller)
- `buflen`: Size of the output buffer (recommended: 32 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - `snprintf` (standard C library function)
- Called from (representative examples):
  - [printVersion](../p/printVersion.md) (src/bin/pgbench/pgbench.c:6373)
  - [connection_warnings](../c/connection_warnings.md) (src/bin/psql/command.c:3929, 3950, 3952)
  - [SyncVariables](../S/SyncVariables.md) (src/bin/psql/command.c:4064)
  - [describeAccessMethods](../d/describeAccessMethods.md) (src/bin/psql/describe.c:153)

## Notes and Other Information
- Encapsulates knowledge of PostgreSQL's version numbering scheme changes
- Thread-safe due to caller-provided buffer design
- Buffer size recommendation of 32 bytes accounts for longest possible version strings
- Returns the buffer address as a convenience for chaining operations
- Critical for client tools that need to display or log version information
- Handles version comparison logic by understanding the encoding differences between old and new schemes

## Simplified Source

```c
char *formatPGVersionNumber(int version_number, bool include_minor, char *buf, size_t buflen) {
    if (version_number >= 100000) {
        // New two-part style (PostgreSQL 10+): major.minor
        if (include_minor)
            snprintf(buf, buflen, "%d.%d", version_number / 10000, version_number % 10000);
        else
            snprintf(buf, buflen, "%d", version_number / 10000);
    } else {
        // Old three-part style (pre-10): major.minor.patch
        if (include_minor)
            snprintf(buf, buflen, "%d.%d.%d", version_number / 10000,
                    (version_number / 100) % 100, version_number % 100);
        else
            snprintf(buf, buflen, "%d.%d", version_number / 10000,
                    (version_number / 100) % 100);
    }
    return buf;
}
```