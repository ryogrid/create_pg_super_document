# pg_walfile_name

## Location
[src/backend/access/transam/xlogfuncs.c:437-461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L437-L461)

## Overview
Computes and returns the WAL (Write-Ahead Log) filename for a given WAL location, such as those returned by pg_backup_stop() or pg_switch_wal().

## Definition


## Detailed Description
This SQL-callable function takes a WAL location (LSN - Log Sequence Number) as input and converts it into the corresponding WAL filename. The function performs several key operations:

1. Validates that the database is not in recovery mode, as WAL filename computation should not be performed during recovery
2. Converts the input LSN to a WAL segment number using the current WAL segment size
3. Constructs the actual WAL filename using the current WAL insertion timeline and segment number
4. Returns the filename as a PostgreSQL text value

The function is essential for backup and recovery operations where external tools need to know the specific WAL filenames corresponding to certain WAL positions.

## Parameters / Member Variables
- Input parameter (via PG_FUNCTION_ARGS):
  - WAL location (LSN): The Log Sequence Number for which to compute the corresponding WAL filename

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSN: Extracts LSN argument from function call
  - [RecoveryInProgress](../R/RecoveryInProgress.md): Checks if database is in recovery mode
  - XLByteToSeg: Converts LSN to WAL segment number
  - [GetWALInsertionTimeLine](../G/GetWALInsertionTimeLine.md): Gets the current WAL timeline ID
  - [XLogFileName](../X/XLogFileName.md): Constructs the WAL filename from timeline and segment
  - cstring_to_text: Converts C string to PostgreSQL text type
  - PG_RETURN_TEXT_P: Returns PostgreSQL text value
- Types used:
  - XLogSegNo: WAL segment number type
  - MAXFNAMELEN: Maximum filename length constant

## Notes and Other Information
- This function cannot be executed during database recovery, as indicated by the RecoveryInProgress() check
- The function uses the current WAL segment size (wal_segment_size) for segment number calculation
- WAL filenames follow a specific format that includes timeline ID and segment number
- Commonly used in backup and recovery scripts to determine which WAL files are needed for point-in-time recovery
- Located in src/backend/access/transam/xlogfuncs.c:437-461