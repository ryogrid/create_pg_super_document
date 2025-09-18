# XLogRecordBuffer

## Location
src/include/replication/decode.h: 17 - 22

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
  - XLogReaderState (structure for WAL record reading state)
- Called from (representative examples):
  - LogicalDecodingProcessRecord
  - xlog_decode
  - xact_decode
  - standby_decode
  - heap2_decode
  - heap_decode
  - logicalmsg_decode
  - DecodeCommit
  - DecodePrepare
  - DecodeAbort
  - DecodeInsert
  - DecodeUpdate
  - DecodeDelete
  - DecodeTruncate
  - DecodeMultiInsert
  - DecodeSpecConfirm
  - DecodeTXNNeedSkip

## Notes and Other Information
- This structure is primarily used within the logical decoding framework (src/backend/replication/logical/)
- The structure provides a convenient way to pass WAL record information between different decoding functions
- The origptr and endptr members allow for precise tracking of record boundaries in the WAL stream
- Used extensively in logical replication where WAL records must be interpreted and converted to logical changes
- Part of PostgreSQL's logical replication infrastructure introduced to support streaming replication of logical changes rather than physical WAL records