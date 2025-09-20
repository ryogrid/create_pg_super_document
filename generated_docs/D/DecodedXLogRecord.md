# DecodedXLogRecord

## Location
[src/include/access/xlogreader.h:156-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlogreader.h#L156-L173)

## Overview
DecodedXLogRecord represents the parsed and decoded contents of a WAL record, providing structured access to WAL record data including metadata, transaction information, and backup block data.

## Definition

```c
typedef struct DecodedXLogRecord
{
	/* Private member used for resource management. */
	size_t		size;			/* total size of decoded record */
	bool		oversized;		/* outside the regular decode buffer? */
	struct DecodedXLogRecord *next; /* decoded record queue link */

	/* Public members. */
	XLogRecPtr	lsn;			/* location */
	XLogRecPtr	next_lsn;		/* location of next record */
	XLogRecord	header;			/* header */
	RepOriginId record_origin;
	TransactionId toplevel_xid; /* XID of top-level transaction */
	char	   *main_data;		/* record's main data portion */
	uint32		main_data_len;	/* main data portion's length */
	int			max_block_id;	/* highest block_id in use (-1 if none) */
	DecodedBkpBlock blocks[FLEXIBLE_ARRAY_MEMBER];
} DecodedXLogRecord;
```
## Detailed Description
DecodedXLogRecord is the fundamental structure representing a fully parsed WAL record in PostgreSQL. It transforms the binary WAL format into an accessible structure that separates metadata, transaction context, main data payload, and backup block information. The structure uses a flexible design where the main_data and block data are stored in contiguous memory after the structure itself, with pointers providing access to these variable-length sections. This design enables efficient memory usage and simplifies record management while providing structured access to all components of a WAL record for replay, logical replication, and analysis purposes.

## Parameters / Member Variables
- `size`: Total size in bytes of the entire decoded record including all variable-length data, used for memory management
- `oversized`: Boolean flag indicating whether this record exceeds the regular decode buffer size and was allocated separately
- `*next`: Linked list pointer for chaining decoded records in the decode queue, enabling efficient sequential processing
- `lsn`: WAL Log Sequence Number indicating the exact position where this record starts in the WAL stream
- `next_lsn`: LSN position where the next WAL record begins, derived from current record's position plus its length
- `header`: Complete XLogRecord header containing record type, length, transaction ID, and other core metadata
- `record_origin`: Replication origin identifier for tracking records in multi-master replication scenarios
- `toplevel_xid`: Transaction ID of the top-level transaction, important for logical replication and transaction tracking
- `*main_data`: Pointer to the primary data payload of the WAL record, containing operation-specific information
- `main_data_len`: Length in bytes of the main data portion, essential for proper data interpretation
- `max_block_id`: Highest block identifier used in this record's backup blocks (-1 if no blocks), indicating data page modifications
- `blocks[FLEXIBLE_ARRAY_MEMBER]`: Flexible array of DecodedBkpBlock structures containing full-page images and delta information for modified data pages
## Dependencies
- Functions called/Symbols referenced:
  - [XLogRecord](../X/XLogRecord.md) (WAL record header structure)
  - RepOriginId (replication origin identifier type)
  - TransactionId (transaction identifier type)
  - DecodedBkpBlock (decoded backup block structure)
  - FLEXIBLE_ARRAY_MEMBER (C99 flexible array member specification)
- Called from (representative examples):
  - [XLogInsertRecord](../X/XLogInsertRecord.md) (record insertion operations)
  - [XLogPrefetcher](../X/XLogPrefetcher.md) (WAL prefetching infrastructure)
  - [XLogReadRecord](../X/XLogReadRecord.md) (record reading operations)
  - [XLogDecodeNextRecord](../X/XLogDecodeNextRecord.md) (record decoding operations)
  - [XLogReaderState](../X/XLogReaderState.md) (embedded within reader state for current record)
  - Various WAL processing and replay functions throughout PostgreSQL

## Notes and Other Information
This structure represents the culmination of WAL record parsing and provides the interface between raw WAL data and PostgreSQL's recovery/replication systems. The contiguous memory layout with trailing variable-length data ensures efficient memory usage and cache-friendly access patterns. The structure supports both regular records that fit in the standard decode buffer and oversized records that require separate allocation. The backup block array is particularly important for crash recovery as it contains full-page images needed to ensure data consistency during replay operations. The record serves as input to various subsystems including crash recovery, streaming replication, logical decoding, and backup tools.