# logicalmsg_desc

## Location
[src/backend/access/rmgrdesc/logicalmsgdesc.c:19-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/logicalmsgdesc.c#L19-L45)

## Overview
A function that generates human-readable descriptions of logical message WAL (Write-Ahead Log) records for debugging and diagnostic purposes.

## Definition

```c
void
logicalmsg_desc(StringInfo buf, XLogReaderState *record)
```
## Detailed Description
The `logicalmsg_desc` function is part of PostgreSQL's WAL record description system, specifically designed to decode and format logical message records for human consumption. This function extracts information from a logical message WAL record and formats it into a readable string that includes whether the message is transactional, the message prefix, and the payload data as hexadecimal bytes. It's primarily used by debugging tools like `pg_waldump` to provide meaningful descriptions of WAL records.

The function handles the `XLOG_LOGICAL_MESSAGE` record type, parsing the embedded `xl_logical_message` structure to extract the prefix string and message payload. The payload is displayed as a series of space-separated hexadecimal bytes for easy inspection.

## Parameters / Member Variables
- `buf`: A StringInfo buffer where the formatted description will be appended
- `record`: An XLogReaderState pointer containing the WAL record to be described

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData - Retrieves the data portion of the WAL record
  - XLogRecGetInfo - Gets the info field from the WAL record
  - XLR_INFO_MASK - Mask used to filter info bits
  - XLOG_LOGICAL_MESSAGE - Constant identifying logical message record type
  - [xl_logical_message](../x/xl_logical_message.md) - Structure representing logical message data

- Called from (representative examples):
  - WAL record description infrastructure
  - pg_waldump utility for WAL analysis

## Notes and Other Information
- The function only processes records with info type `XLOG_LOGICAL_MESSAGE`
- The message prefix is null-terminated and displayed as a quoted string
- The payload is formatted as hexadecimal bytes regardless of actual content type
- Distinguishes between transactional and non-transactional logical messages
- Part of the resource manager description framework for WAL record interpretation

## Simplified Source

```c
void logicalmsg_desc(StringInfo buf, XLogReaderState *record) {
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    if (info == XLOG_LOGICAL_MESSAGE) {
        xl_logical_message *xlrec = (xl_logical_message *) rec;
        char *prefix = xlrec->message;
        char *message = xlrec->message + xlrec->prefix_size;
        char *sep = "";

        // Format the logical message description
        appendStringInfo(buf, "%s, prefix \"%s\"; payload (%zu bytes): ",
                         xlrec->transactional ? "transactional" : "non-transactional",
                         prefix, xlrec->message_size);

        // Output message payload as hex bytes
        for (int cnt = 0; cnt < xlrec->message_size; cnt++) {
            appendStringInfo(buf, "%s%02X", sep, (unsigned char) message[cnt]);
            sep = " ";
        }
    }
}
```