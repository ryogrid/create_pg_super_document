# XLogReaderState

## Location
[src/include/access/xlogreader.h:59-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlogreader.h#L59-L71)

## Overview
XLogReaderState is the central state management structure for PostgreSQL's WAL (Write Ahead Log) reading infrastructure, providing a comprehensive context for sequentially reading and decoding WAL records.

## Definition


## Detailed Description
XLogReaderState serves as the comprehensive state machine for WAL reading operations in PostgreSQL. It manages the complex process of reading WAL records sequentially from disk, handling page boundaries, timeline switches, and decoding binary WAL data into usable structures. The structure maintains multiple levels of state: positioning information for tracking where we are in the WAL stream, buffering for efficient I/O operations, decoded record queues for managing parsed data, and error handling for robust operation. It supports both blocking and non-blocking operation modes and handles complex scenarios like timeline switches during point-in-time recovery.

## Parameters / Member Variables
- : Callback functions for customizable WAL reading operations (page reading, segment opening/closing)
- : Unique identifier for the PostgreSQL system, used for validation
- : Opaque pointer for callback functions to store custom state
- /: Position tracking for the last record read from WAL
- //: Recovery state tracking for handling incomplete records
- //: Position tracking for decoded records
- : Pointer to the most recently decoded and returned WAL record
- : Circular buffer management for decoded records to enable efficient record queuing
- /: Queue of decoded records ready for consumption
- /: Page-level buffer for raw WAL data read from disk
- //: WAL segment context and current open segment state
- /: Timeline validation state for the most recent page read
- ///: Current position and timeline management for handling timeline switches
- /: Expandable buffer for assembling records that span multiple pages
- /: Error message handling and deferred error reporting
- : Flag controlling whether operations should block waiting for data

## Dependencies
- Functions called/Symbols referenced:
  - [XLogReaderRoutine](XLogReaderRoutine.md) (callback structure)
  - [DecodedXLogRecord](../D/DecodedXLogRecord.md) (decoded record structure)
  - [WALSegmentContext](../W/WALSegmentContext.md) (segment context)
  - [WALOpenSegment](../W/WALOpenSegment.md) (open segment state)
  - XLogRecPtr (WAL position type)
  - TimeLineID (timeline identifier type)
- Called from (representative examples):
  - [XLogReaderAllocate](XLogReaderAllocate.md) (allocation function)
  - [XLogReaderFree](XLogReaderFree.md) (deallocation function)
  - [XLogBeginRead](XLogBeginRead.md) (positioning function)
  - [XLogFindNextRecord](XLogFindNextRecord.md) (navigation function)
  - Various callback functions throughout the WAL reading infrastructure

## Notes and Other Information
This structure represents the heart of PostgreSQL's WAL reading infrastructure and is used throughout the system for WAL replay, logical replication, backup tools, and recovery processes. The dual-buffer design (page-level readBuf and record-level decode buffers) enables efficient handling of records that span page boundaries while maintaining good performance. The timeline management features are crucial for point-in-time recovery scenarios where WAL reading may need to switch between different timeline branches. The structure supports both synchronous and asynchronous operation modes through the nonblocking flag, making it suitable for different use cases from recovery to streaming replication.