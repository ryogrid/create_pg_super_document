# XLogRecord

## Location
src/include/access/xlogrecord.h: 41 - 53

## Overview
XLogRecord is the fundamental data structure that represents the fixed-size header of every WAL (Write-Ahead Logging) record in PostgreSQL. It contains essential metadata for transaction logging, recovery, and replication operations.

## Definition


## Detailed Description
XLogRecord serves as the header structure for every WAL record in PostgreSQL's transaction log. It provides critical metadata that enables the system to process, validate, and replay transaction records during normal operation, crash recovery, and replication. The structure is designed to be compact and efficient, as it appears at the beginning of every WAL record. The layout ensures proper alignment on MAXALIGN boundaries in WAL files, and the structure is followed by variable-length data including block headers, data headers, and the actual logged data.

## Parameters / Member Variables
- : Total length of the entire WAL record including this header and all associated data
- : Transaction ID that generated this WAL record, used for transaction tracking and recovery
- : Pointer to the previous WAL record in the log, forming a linked chain for sequential access
- : Flag bits containing record-specific information and metadata
- : Resource manager ID that identifies which subsystem generated this record (heap, btree, etc.)
- : CRC32C checksum for data integrity verification of this entire record

## Dependencies
- Functions called/Symbols referenced:
  - RmgrId
  - pg_crc32c

- Called from (representative examples):
  - [XLogInsertRecord](XLogInsertRecord.md)
  - [XLogRecordAssemble](XLogRecordAssemble.md)
  - [XLogDecodeNextRecord](XLogDecodeNextRecord.md)
  - [ValidXLogRecordHeader](../V/ValidXLogRecordHeader.md)
  - [ValidXLogRecord](../V/ValidXLogRecord.md)
  - DecodeXLogRecord

## Notes and Other Information
- [XLogRecord](XLogRecord.md) structs always start on MAXALIGN boundaries in WAL files
- The structure is followed by variable-length components: XLogRecordBlockHeaders, XLogRecordDataHeaders, block data, and main data
- The padding between xl_rmid and xl_crc must be initialized to zero
- This is the foundation structure for PostgreSQL's WAL-based durability and replication systems
- The CRC field enables detection of corruption in WAL records during recovery operations