# brin_xlog_insert

## Location
[src/backend/access/brin/brin_xlog.c:124-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_xlog.c#L124-L134)

## Overview
A static function that handles WAL replay for BRIN index insertion operations during crash recovery.

## Definition
```c
static void brin_xlog_insert(XLogReaderState *record)
```

## Detailed Description
This function serves as a wrapper for BRIN index insertion replay operations. It extracts the insertion-specific data from the WAL record and delegates the actual work to the shared brin_xlog_insert_update function. This design pattern allows code reuse between insertion and update operations while maintaining clear separation of concerns for different WAL record types.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record data for the BRIN insertion operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extracts the data portion from the WAL record
  - [brin_xlog_insert_update](brin_xlog_insert_update.md): Shared function that performs the actual insertion work
  - [xl_brin_insert](../x/xl_brin_insert.md): Structure containing BRIN insertion parameters
- Called from (representative examples):
  - [brin_redo](brin_redo.md): Main BRIN WAL replay dispatcher function

## Notes and Other Information
- This is a static function only accessible within the brin_xlog.c file
- Acts as a thin wrapper around brin_xlog_insert_update for code organization
- Part of PostgreSQL's crash recovery mechanism for BRIN indexes
- Located at src/backend/access/brin/brin_xlog.c:124-134
- Very concise implementation that primarily serves as an adapter between the WAL replay dispatcher and the shared insertion/update logic

## Simplified Source

```c
static void brin_xlog_insert(XLogReaderState *record)
{
    // Extract BRIN insertion data from WAL record
    xl_brin_insert *insert_data = (xl_brin_insert *) XLogRecGetData(record);

    // Delegate to shared insertion/update logic
    brin_xlog_insert_update(record, insert_data);
}
```