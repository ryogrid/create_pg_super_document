# XLogRecordDataHeaderLong

## Location
[src/include/access/xlogrecord.h:221-225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlogrecord.h#L221-L225)

## Overview
XLogRecordDataHeaderLong is a header structure used for the "main data" portion of WAL records when the data length exceeds 255 bytes, providing a larger length field compared to the short form.

## Definition

```c
typedef struct XLogRecordDataHeaderLong
{
	uint8		id;				/* XLR_BLOCK_ID_DATA_LONG */
	/* followed by uint32 data_length, unaligned */
}			XLogRecordDataHeaderLong;
```
## Detailed Description
XLogRecordDataHeaderLong is one of two possible headers for the main data portion of WAL records, specifically used when the data payload exceeds 255 bytes. It forms part of PostgreSQL's space-efficient WAL format that uses variable-length headers based on data size. The structure includes an ID field that identifies it as the long form header, followed by an unaligned uint32 data_length field that can accommodate much larger data sizes than the single-byte length field in XLogRecordDataHeaderShort. This design optimizes WAL storage by using the minimal header size appropriate for the data length.

## Parameters / Member Variables
- `id`: Identifier set to XLR_BLOCK_ID_DATA_LONG to distinguish this as the long form data header
- `data_length`: (Following field) 32-bit unsigned integer containing the number of payload bytes in the main data (stored unaligned after the id field)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)

- Called from (representative examples):
  - (No direct references found in current codebase - used for documentation purposes)

## Notes and Other Information
- Used when main data payload is 256 bytes or larger (otherwise XLogRecordDataHeaderShort is used)
- The data_length field is stored unaligned immediately after the id field for space efficiency
- Part of PostgreSQL's variable-length WAL record format that minimizes storage overhead
- According to source comments, these structs are currently used mainly for documentation purposes
- Forms the boundary between WAL record headers and the actual payload data
- The long form allows for data payloads up to 4GB in size (uint32 range)
- Complements XLogRecordDataHeaderShort in providing size-appropriate headers for different data lengths