# ready_file_comparator

## Location
[src/backend/postmaster/pgarch.c:779-801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/pgarch.c#L779-L801)

## Overview
A static comparison function used to determine archival priority between WAL files when sorting them for the PostgreSQL archiver process.

## Definition
```c
static int ready_file_comparator(Datum a, Datum b, void *arg)
```

## Detailed Description
The `ready_file_comparator` function implements a comparison algorithm that establishes priority rules for archiving WAL (Write-Ahead Log) files. It follows a two-tier priority system:

1. **Timeline history files have absolute priority**: Files identified as timeline history files (via `IsTLHistoryFileName`) are always given higher priority regardless of their names.

2. **Lexicographic ordering for regular files**: For files of the same type (both timeline history or both regular WAL files), priority is determined by string comparison, giving preference to "older" files based on lexicographic ordering.

The function is designed to work with PostgreSQLs sorting infrastructure, returning negative values when the first argument has higher priority, positive values when the second argument has higher priority, and zero for equivalent priority.

## Parameters / Member Variables
- `a`: First Datum containing a C string filename to compare
- `b`: Second Datum containing a C string filename to compare  
- `arg`: Unused void pointer argument (required for compatibility with sorting interface)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetCString](../D/DatumGetCString.md): Extracts C string from Datum values
  - `IsTLHistoryFileName`: Checks if a filename represents a timeline history file
  - `strcmp`: Standard C library string comparison function

- Called from (representative examples):
  - [pgarch_readyXlog](../p/pgarch_readyXlog.md): Uses this comparator when sorting WAL files for archival
  - [PgArchiverMain](../P/PgArchiverMain.md): Part of the archiver main loop that processes files in priority order
  - [arch_files_state](../a/arch_files_state.md): Used in the archival file state management

## Notes and Other Information
- This is a static function, meaning its only accessible within the pgarch.c source file
- The priority system ensures critical timeline history files are archived first, which is essential for point-in-time recovery scenarios
- The lexicographic ordering ensures consistent, predictable archival order for WAL segments
- The function signature matches the requirements for use with PostgreSQLs internal sorting routines