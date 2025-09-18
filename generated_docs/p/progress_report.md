# progress_report

## Location
src/bin/pg_checksums/pg_checksums.c: 124 - 157

## Overview
The `progress_report` function displays real-time progress information during database relation and page checking operations in the pg_amcheck utility, showing completion percentages and current database being processed.

## Definition
```c
static void progress_report(uint64 relations_total, uint64 relations_checked,
                           uint64 relpages_total, uint64 relpages_checked,
                           const char *datname, bool force, bool finished)
```

## Detailed Description
This function provides a sophisticated progress reporting mechanism for the pg_amcheck utility, which is used to verify the integrity of PostgreSQL database relations. The function:

1. **Rate limiting**: Reports progress at most once per second unless forced or finished
2. **Dual metrics**: Tracks both relation-level and page-level progress with separate percentages
3. **Adaptive display**: Shows different levels of detail based on verbose mode settings
4. **Terminal awareness**: Uses carriage return for terminal output to update the same line, or newline for non-terminal output
5. **Database context**: Optionally displays the current database name being processed with intelligent truncation

The function formats progress as both absolute numbers and percentages, making it easy for users to understand both current position and completion rate. In verbose mode, it includes the database name with proper truncation if the name is too long.

## Parameters / Member Variables
- `relations_total`: Total number of relations to be checked
- `relations_checked`: Number of relations already processed  
- `relpages_total`: Total number of relation pages to be checked
- `relpages_checked`: Number of relation pages already processed
- `datname`: Name of the current database being processed (can be NULL)
- `force`: If true, bypasses the once-per-second rate limiting
- `finished`: If true, indicates this is the final progress report and moves cursor to next line

## Dependencies
- Functions called/Symbols referenced:
  - `time` (standard C library function for getting current time)
  - `snprintf` (for formatting numeric values)
  - `fprintf` (for outputting progress information)
  - `strlen` (for string length calculations)
  - `isatty` and `fileno` (for terminal detection)
  - `fputc` (for outputting carriage return or newline)
  - `pg_time_t` (PostgreSQL time type)
  - `UINT64_FORMAT` (PostgreSQL macro for formatting uint64 values)
  - Global variables: `opts.show_progress`, `opts.verbose`, `last_progress_report`, `progress_since_last_stderr`

- Called from (representative examples):
  - `[main](../m/main.md)` function in pg_amcheck.c during relation checking loops
  - `[scan_file](../s/scan_file.md)` function in pg_checksums.c
  - `[ReceiveArchiveStreamChunk](../R/ReceiveArchiveStreamChunk.md)` and related functions in pg_basebackup.c
  - `[verify_backup_checksums](../v/verify_backup_checksums.md)` in pg_verifybackup.c

## Notes and Other Information
- This is a static function with internal linkage, accessible only within its source file
- Uses internationalization support through the `_()` macro for translatable progress messages
- Implements intelligent terminal handling: uses `\r` (carriage return) for terminals to overwrite the current line, and `\n` (newline) for non-terminals
- The `VERBOSE_DATNAME_LENGTH` constant (35 characters) defines the maximum display width for database names
- Database name truncation uses leading truncation with "..." prefix when the name exceeds the display width
- Progress reporting can be completely disabled via the `opts.show_progress` flag
- The function maintains global state through `last_progress_report` to implement rate limiting