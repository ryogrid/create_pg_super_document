# standby_desc_running_xacts

## Location
[src/backend/access/rmgrdesc/standbydesc.c:20-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/standbydesc.c#L20-L46)

## Overview
A static function that formats running transaction information from write-ahead log records into human-readable strings for debugging and monitoring purposes.

## Definition


## Detailed Description
This function takes a  WAL record and formats its contents into a human-readable string representation. It extracts and displays information about currently running transactions, including transaction IDs, completion status, and subtransaction details. The function is primarily used for WAL record description and debugging purposes in PostgreSQL's standby/replication functionality.

The function formats the output to show:
- Next transaction ID to be assigned
- Latest completed transaction ID  
- Oldest currently running transaction ID
- List of currently running transaction IDs (if any)
- Subtransaction overflow status
- List of subtransaction IDs (if any)

## Parameters / Member Variables
- `buf`: StringInfo buffer to append the formatted description to
- `xlrec`: Pointer to xl_running_xacts structure containing the transaction information from the WAL record

## Dependencies
- Functions called/Symbols referenced:
  - xl_running_xacts (struct type)
  - appendStringInfo
  - appendStringInfoString
- Called from (representative examples):
  - [standby_desc](standby_desc.md)

## Notes and Other Information
- This is a static function, only accessible within the standbydesc.c file
- Part of the WAL record description infrastructure for standby operations
- Handles both main transactions and subtransactions
- Includes overflow detection for subtransaction arrays
- Output format is designed for human readability in log files and debugging tools