# dumpTimestamp

## Location
[src/bin/pg_dump/pg_backup_archiver.c:4136-4153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L4136-L4153)

## Overview
Outputs a formatted timestamp comment to an archive handle for PostgreSQL dump files.

## Definition


## Detailed Description
This function formats and outputs timestamp information as SQL comments in PostgreSQL dump files. It converts a Unix timestamp to a human-readable format using the standard format "%Y-%m-%d %H:%M:%S %Z" and writes it to the archive handle with a descriptive message. The output appears as SQL comments (lines beginning with --) in the dump file, providing timing information for various dump operations.

## Parameters / Member Variables
- `AH`: Archive handle representing the output destination for the dump
- `msg`: Descriptive message to accompany the timestamp (e.g., "Dump started on", "Dump completed on")
- `tim`: Unix timestamp (time_t) to be formatted and output

## Dependencies
- Functions called/Symbols referenced:
  - strftime (standard C library function for time formatting)
  - PGDUMP_STRFTIME_FMT (format string macro defined as "%Y-%m-%d %H:%M:%S %Z")
  - [ahprintf](../a/ahprintf.md) (archive-specific printf function for formatted output)
- Called from (representative examples):
  - [RestoreArchive](../R/RestoreArchive.md) (multiple calls for timing restoration operations)
  - TEXT_DUMPALL_HEADER (for dump file header timestamps)
  - [main](../m/main.md) functions in pg_dumpall.c (for overall dump timing)

## Notes and Other Information
- The function is static and only accessible within pg_backup_archiver.c
- Uses a 64-byte buffer for timestamp formatting, which is sufficient for the standard format
- If strftime fails to format the timestamp (returns 0), no output is produced
- The timestamp format includes timezone information (%Z) for better temporal context
- Output format follows SQL comment syntax with "-- " prefix for compatibility with SQL parsers