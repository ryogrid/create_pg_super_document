# logicalmsg_identify

## Location
[src/backend/access/rmgrdesc/logicalmsgdesc.c:46-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/logicalmsgdesc.c#L46-L52)

## Overview
A function that returns a string identifier for logical message WAL record types, used in PostgreSQL's WAL record identification system.

## Definition
```c
const char *logicalmsg_identify(uint8 info)
```

## Detailed Description
The `logicalmsg_identify` function is part of PostgreSQL's resource manager framework for WAL (Write-Ahead Log) record processing. It takes an info byte from a WAL record and returns a human-readable string identifier if the record type corresponds to a logical message. This function is typically used by WAL analysis tools and debugging utilities to provide meaningful names for different types of WAL records.

The function performs a simple check to determine if the provided info byte (with info mask bits cleared) matches the `XLOG_LOGICAL_MESSAGE` constant. If it matches, it returns the string "MESSAGE"; otherwise, it returns NULL to indicate the record type is not handled by this resource manager.

## Parameters / Member Variables
- `info`: An 8-bit unsigned integer containing the info field from a WAL record, which encodes the specific operation type

## Dependencies
- Functions called/Symbols referenced:
  - XLR_INFO_MASK - Mask used to clear info bits that are not part of the record type
  - XLOG_LOGICAL_MESSAGE - Constant identifying logical message record type

- Called from (representative examples):
  - WAL record identification infrastructure
  - Resource manager dispatch systems
  - WAL analysis and debugging tools

## Notes and Other Information
- Returns "MESSAGE" for `XLOG_LOGICAL_MESSAGE` record types
- Returns NULL for unrecognized record types
- Part of the resource manager interface for logical message handling
- The function masks out non-essential info bits before comparison
- Used primarily for diagnostic and debugging purposes in WAL processing