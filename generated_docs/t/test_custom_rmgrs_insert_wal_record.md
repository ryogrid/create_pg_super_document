# test_custom_rmgrs_insert_wal_record

## Location
[src/test/modules/test_custom_rmgrs/test_custom_rmgrs.c:120-140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_custom_rmgrs/test_custom_rmgrs.c#L120-L140)

## Overview
A PostgreSQL SQL function that inserts a custom WAL record containing a text message using the test custom resource manager.

## Definition

```c
Datum
test_custom_rmgrs_insert_wal_record(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL SQL-callable function that demonstrates how to insert custom WAL records using a custom resource manager. The function takes a text argument and writes it to the WAL as a custom record type.

The function performs the following operations:
1. Extracts the text argument from the SQL function call using PostgreSQL's function argument macros
2. Creates an  record structure with the message size
3. Begins a WAL insert operation using 
4. Registers the record header and payload data with the WAL system
5. Marks the record as unimportant (not critical for recovery) using 
6. Inserts the complete WAL record using the custom resource manager ID
7. Returns the LSN (Log Sequence Number) where the record was written

This function serves as a test utility for validating the custom resource manager functionality and demonstrates the proper way to write custom WAL records in PostgreSQL extensions.

## Parameters / Member Variables
The function follows PostgreSQL's SQL function interface:
- Uses  macro to access arguments
- First argument (index 0): A  value containing the message to be logged

## Dependencies
- Functions called/Symbols referenced:
  -  (extracts text argument from SQL call)
  -  (gets pointer to variable-length data)
  -  (gets size of variable-length data excluding header)
  -  (WAL record structure)
  -  (begins WAL insert operation)
  -  (registers data chunks for WAL record)
  -  (constant for record header size)
  -  (sets WAL record flags)
  -  (flag for non-critical records)
  -  (commits WAL record)
  -  (custom resource manager ID)
  -  (record operation code)
  -  (returns LSN value to SQL caller)
- Called from (representative examples):
  - SQL queries: 
  - PostgreSQL test cases and debugging scenarios

## Notes and Other Information
- This is a SQL-callable function exposed to PostgreSQL users for testing purposes
- The function is declared with  macro for PostgreSQL function registration
- WAL records created by this function are marked as unimportant and won't affect recovery semantics
- Returns the LSN where the record was inserted, allowing callers to track the WAL position
- Located in
- Part of the test custom resource manager module for validating custom WAL resource manager functionality
- The function demonstrates proper use of PostgreSQL's WAL insertion API for custom extensions

## Simplified Source

```c
Datum
test_custom_rmgrs_insert_wal_record(PG_FUNCTION_ARGS)
{
    // Extract text argument and its data
    text *arg = PG_GETARG_TEXT_PP(0);
    char *payload = VARDATA_ANY(arg);
    Size len = VARSIZE_ANY_EXHDR(arg);

    // Prepare WAL record structure
    xl_testcustomrmgrs_message xlrec;
    xlrec.message_size = len;

    // Begin WAL insertion and register data
    XLogBeginInsert();
    XLogRegisterData((char *) &xlrec, SizeOfTestCustomRmgrsMessage);
    XLogRegisterData((char *) payload, len);

    // Mark as unimportant and insert
    XLogSetRecordFlags(XLOG_MARK_UNIMPORTANT);
    XLogRecPtr lsn = XLogInsert(RM_TESTCUSTOMRMGRS_ID, XLOG_TEST_CUSTOM_RMGRS_MESSAGE);

    PG_RETURN_LSN(lsn);
}
```