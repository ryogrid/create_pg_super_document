# XLogFromFileName

## Location
[src/include/access/xlog_internal.h:200-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L200-L209)

## Overview
XLogFromFileName parses a WAL segment filename to extract the timeline ID and logical segment number components.

## Definition


## Detailed Description
XLogFromFileName performs the reverse operation of XLogFileName by parsing a WAL segment filename and extracting its constituent parts. It uses sscanf to parse the hexadecimal timeline ID, log file number, and segment number from the filename, then calculates the logical segment number by combining the log and segment components using the WAL segment size. This function is essential for converting WAL filenames back into their logical representation for processing and comparison operations.

## Parameters / Member Variables
- : Input WAL segment filename to parse
- : Output pointer to store the extracted timeline ID
- : Output pointer to store the calculated logical segment number
- : WAL segment size in bytes, used to calculate the logical segment number

## Dependencies
- Functions called/Symbols referenced:
  - XLogSegmentsPerXLogId
  - sscanf (standard C library)
- Called from (representative examples):
  - [XLogGetOldestSegno](XLogGetOldestSegno.md)
  - [UpdateLastRemovedPtr](../U/UpdateLastRemovedPtr.md)
  - [perform_base_backup](../p/perform_base_backup.md)
  - [FindStreamingStart](../F/FindStreamingStart.md)
  - [main](../m/main.md) (in pg_waldump)

## Notes and Other Information
- This is an inline function defined in xlog_internal.h for performance
- Inverse operation of XLogFileName - parses filenames back to logical components
- Critical for WAL file analysis, backup operations, and streaming replication
- Assumes the input filename is a valid WAL segment name (should be validated with IsXLogFileName first)
- The logical segment number calculation accounts for the variable WAL segment size