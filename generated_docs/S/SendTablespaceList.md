# SendTablespaceList

## Location
src/backend/backup/basebackup_copy.c: 378 - 422

## Overview
SendTablespaceList is a static function that sends a result set describing the tablespace list via the PostgreSQL libpq protocol during base backup operations.

## Definition
```c
static void SendTablespaceList(List *tablespaces)
```

## Detailed Description
This function creates and sends a three-column result set containing information about tablespaces involved in a base backup operation. It iterates through a list of tablespace information structures and sends each tablespace's OID, location path, and size. The function handles NULL values appropriately when tablespace paths are not available or sizes are unknown. Sizes are converted from bytes to kilobytes before transmission.

## Parameters / Member Variables
- `tablespaces`: List * - A linked list containing tablespaceinfo structures with details about each tablespace

## Dependencies
- Functions called/Symbols referenced:
  - CreateDestReceiver
  - DestRemoteSimple
  - CreateTemplateTupleDesc
  - TupleDescInitBuiltinEntry
  - begin_tup_output_tupdesc
  - tablespaceinfo (struct type)
  - ObjectIdGetDatum
  - CStringGetTextDatum
  - Int64GetDatum
  - do_tup_output
  - end_tup_output
- Called from (representative examples):
  - bbsink_copystream_begin_backup

## Notes and Other Information
- This is a static function limited to the basebackup_copy.c file
- Creates a 3-column result set with 'spcoid' (OID), 'spclocation' (TEXT), and 'size' (INT8)
- Handles NULL values for tablespaces with missing path information
- Converts tablespace sizes from bytes to kilobytes before sending
- Size values of -1 are treated as NULL (unknown size)
- Uses the standard PostgreSQL tuple output mechanism for sending structured data
- Essential for communicating tablespace layout information during base backup initialization