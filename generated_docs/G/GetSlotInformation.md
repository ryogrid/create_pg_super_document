# GetSlotInformation

## Location
src/bin/pg_basebackup/streamutil.c: 561 - 654

## Overview
Executes the READ_REPLICATION_SLOT replication command to retrieve information about a specified physical replication slot, including its restart LSN position and current timeline ID.

## Definition


## Detailed Description
This function sends the READ_REPLICATION_SLOT replication protocol command to query information about a named physical replication slot. It validates that the slot exists and is of the correct type (physical), then extracts the restart LSN position and timeline ID from the response. The function is designed to work specifically with physical replication slots and will return an error if the slot is logical or doesn't exist.

## Parameters / Member Variables
- `conn`: PostgreSQL connection handle for the replication command
- `slot_name`: Name of the replication slot to query
- `restart_lsn`: Output parameter for the slot's restart LSN position (optional, can be NULL)
- `restart_tli`: Output parameter for the slot's current timeline ID (optional, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer - Create dynamic string buffer
  - appendPQExpBuffer - Append formatted data to buffer
  - destroyPQExpBuffer - Free buffer memory
  - PQexec - Execute SQL command
  - PQresultStatus - Get result status
  - PQntuples - Get number of result rows
  - PQnfields - Get number of result fields
  - PQgetisnull - Check if field is NULL
  - PQgetvalue - Get field value from result
  - PQclear - Free result memory
  - pg_log_error - Log error messages
  - strcmp - String comparison
  - sscanf - Parse formatted string
  - atol - Convert string to long integer
- Called from (representative examples):
  - StreamLog (pg_receivewal.c:547)

## Notes and Other Information
- The function expects exactly 1 row with 3 fields in the result (slot_type, restart_lsn, restart_tli)
- Only supports physical replication slots - returns error for logical slots
- Returns InvalidXLogRecPtr for restart_lsn and 0 for restart_tli if the slot information is not available
- LSN is parsed from hexadecimal format (X/X) and converted to XLogRecPtr
- If the slot doesn't exist, READ_REPLICATION_SLOT returns a tuple with NULL values
- Both output parameters are optional - callers can pass NULL for information they don't need
- Returns false on any error (connection issues, slot doesn't exist, wrong slot type, parsing errors)