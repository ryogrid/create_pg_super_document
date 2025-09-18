# XLogRecordBuffer

## Location
[src/include/replication/decode.h:17-22](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/decode.h#L17-L22)

## Overview
XLogRecordBuffer is a structure used in PostgreSQL's logical decoding system to encapsulate information about a WAL (Write-Ahead Log) record being processed for replication purposes.

## Definition


## Detailed Description
XLogRecordBuffer serves as a container structure that holds essential information about a WAL record during logical decoding operations. This structure is primarily used in the logical replication subsystem where WAL records need to be decoded and transformed into logical changes that can be replicated to other PostgreSQL instances.

The structure provides both positional information (where the record starts and ends in the WAL) and access to the actual record data through the XLogReaderState pointer. This design allows logical decoding functions to efficiently process WAL records while maintaining knowledge of their exact location in the transaction log.

## Parameters / Member Variables
- : XLogRecPtr pointing to the original starting position of the WAL record in the transaction log
- : XLogRecPtr pointing to the ending position of the WAL record in the transaction log  
- : Pointer to XLogReaderState structure containing the actual WAL record data and reader state information

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtr (typedef for WAL record pointer)
  - [XLogReaderState](XLogReaderState.md) (structure for WAL record reading state)
- Called from (representative examples):
  - [LogicalDecodingProcessRecord](../L/LogicalDecodingProcessRecord.md)
  - [xlog_decode](../x/xlog_decode.md)
  - [xact_decode](../x/xact_decode.md)
  - [standby_decode](../s/standby_decode.md)
  - [heap2_decode](../h/heap2_decode.md)
  - [heap_decode](../h/heap_decode.md)
  - [logicalmsg_decode](../l/logicalmsg_decode.md)
  - [DecodeCommit](../D/DecodeCommit.md)
  - [DecodePrepare](../D/DecodePrepare.md)
  - [DecodeAbort](../D/DecodeAbort.md)
  - [DecodeInsert](../D/DecodeInsert.md)
  - [DecodeUpdate](../D/DecodeUpdate.md)
  - [DecodeDelete](../D/DecodeDelete.md)
  - [DecodeTruncate](../D/DecodeTruncate.md)
  - [DecodeMultiInsert](../D/DecodeMultiInsert.md)
  - [DecodeSpecConfirm](../D/DecodeSpecConfirm.md)
  - [DecodeTXNNeedSkip](../D/DecodeTXNNeedSkip.md)

## Notes and Other Information
- This structure is primarily used within the logical decoding framework (src/backend/replication/logical/)
- The structure provides a convenient way to pass WAL record information between different decoding functions
- The origptr and endptr members allow for precise tracking of record boundaries in the WAL stream
- Used extensively in logical replication where WAL records must be interpreted and converted to logical changes
- Part of PostgreSQL's logical replication infrastructure introduced to support streaming replication of logical changes rather than physical WAL records