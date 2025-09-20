# xl_xact_origin

## Location
[src/include/access/xact.h:308-312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xact.h#L308-L312)

## Overview
Structure that represents transaction origin information in WAL records, containing the LSN and timestamp of the original transaction.

## Definition

```c
typedef struct xl_xact_origin
{
	XLogRecPtr	origin_lsn;
	TimestampTz origin_timestamp;
} xl_xact_origin;
```
## Detailed Description
The xl_xact_origin structure is used to store origin tracking information in WAL records for transaction commit and abort operations. This structure is part of PostgreSQL's logical replication infrastructure, allowing the system to track the original location and timing of replicated transactions. When a transaction is replicated from another PostgreSQL instance, this structure preserves information about where and when the transaction originally occurred.

## Parameters / Member Variables
- `origin_lsn`: The LSN (Log Sequence Number) from the origin server where this transaction was first committed
- `origin_timestamp`: The timestamp when the transaction was originally committed on the origin server
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtr (data type)
  - TimestampTz (data type)
- Called from (representative examples):
  - [ParseCommitRecord](../P/ParseCommitRecord.md) (in xactdesc.c)
  - [ParseAbortRecord](../P/ParseAbortRecord.md) (in xactdesc.c)
  - [XactLogCommitRecord](../X/XactLogCommitRecord.md) (in xact.c)
  - [XactLogAbortRecord](../X/XactLogAbortRecord.md) (in xact.c)

## Notes and Other Information
This structure is essential for logical replication conflict detection and resolution. The origin information helps distinguish between locally generated transactions and those that originated from replication, preventing infinite replication loops and enabling proper conflict resolution in multi-master setups.