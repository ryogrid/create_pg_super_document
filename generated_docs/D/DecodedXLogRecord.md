# DecodedXLogRecord

## Location
src/include/access/xlogreader.h: 156 - 173

## Overview
DecodedXLogRecord represents the parsed and decoded contents of a WAL record, providing structured access to WAL record data including metadata, transaction information, and backup block data.

## Definition


## Detailed Description
DecodedXLogRecord is the fundamental structure representing a fully parsed WAL record in PostgreSQL. It transforms the binary WAL format into an accessible structure that separates metadata, transaction context, main data payload, and backup block information. The structure uses a flexible design where the main_data and block data are stored in contiguous memory after the structure itself, with pointers providing access to these variable-length sections. This design enables efficient memory usage and simplifies record management while providing structured access to all components of a WAL record for replay, logical replication, and analysis purposes.

## Parameters / Member Variables
- : Total size in bytes of the entire decoded record including all variable-length data, used for memory management
- : Boolean flag indicating whether this record exceeds the regular decode buffer size and was allocated separately
- : Linked list pointer for chaining decoded records in the decode queue, enabling efficient sequential processing
- : WAL Log Sequence Number indicating the exact position where this record starts in the WAL stream
- : LSN position where the next WAL record begins, derived from current record's position plus its length
- : Complete XLogRecord header containing record type, length, transaction ID, and other core metadata
- : Replication origin identifier for tracking records in multi-master replication scenarios
- : Transaction ID of the top-level transaction, important for logical replication and transaction tracking
- : Pointer to the primary data payload of the WAL record, containing operation-specific information
- : Length in bytes of the main data portion, essential for proper data interpretation
- : Highest block identifier used in this record's backup blocks (-1 if no blocks), indicating data page modifications
- : Flexible array of DecodedBkpBlock structures containing full-page images and delta information for modified data pages

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecord (WAL record header structure)
  - RepOriginId (replication origin identifier type)
  - TransactionId (transaction identifier type)
  - DecodedBkpBlock (decoded backup block structure)
  - FLEXIBLE_ARRAY_MEMBER (C99 flexible array member specification)
- Called from (representative examples):
  - XLogInsertRecord (record insertion operations)
  - XLogPrefetcher (WAL prefetching infrastructure)
  - XLogReadRecord (record reading operations)
  - XLogDecodeNextRecord (record decoding operations)
  - XLogReaderState (embedded within reader state for current record)
  - Various WAL processing and replay functions throughout PostgreSQL

## Notes and Other Information
This structure represents the culmination of WAL record parsing and provides the interface between raw WAL data and PostgreSQL's recovery/replication systems. The contiguous memory layout with trailing variable-length data ensures efficient memory usage and cache-friendly access patterns. The structure supports both regular records that fit in the standard decode buffer and oversized records that require separate allocation. The backup block array is particularly important for crash recovery as it contains full-page images needed to ensure data consistency during replay operations. The record serves as input to various subsystems including crash recovery, streaming replication, logical decoding, and backup tools.