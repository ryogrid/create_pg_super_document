# SPI_exec

## Location
src/backend/executor/spi.c: 630 - 636

## Overview
SPI_exec is an obsolete version of SPI_execute that provides a simpler interface for executing SQL queries without explicit read-only specification.

## Definition
```c
int SPI_exec(const char *src, long tcount)
```

## Detailed Description
This function is a legacy wrapper around SPI_execute that was maintained for backward compatibility. It simply calls SPI_execute with the read_only parameter hardcoded to false, meaning all queries executed through SPI_exec are treated as potentially read-write operations.

The function exists to maintain compatibility with older code that was written before the read_only parameter was introduced to SPI_execute. It provides the same functionality as SPI_execute but with less control over execution constraints.

## Parameters / Member Variables
- `src`: The SQL query string to execute
- `tcount`: Maximum number of rows to process, or 0 for no limit

## Dependencies
- Functions called/Symbols referenced:
  - SPI_execute (the modern equivalent function)

- Called from (representative examples):
  - refresh_by_match_merge (multiple calls in src/backend/commands/matview.c)

## Notes and Other Information
- Marked as obsolete in the source code comments
- Maintained for backward compatibility only
- Always executes queries in read-write mode (read_only = false)
- New code should use SPI_execute directly for better control
- Very simple implementation - just a thin wrapper
- Located in src/backend/executor/spi.c:630-636
- Still used in some internal PostgreSQL code, particularly in materialized view refresh operations