# XLogRecData

## Location
[src/include/access/xlog_internal.h:312-317](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L312-L317)

## Overview
XLogRecData is a structure used to build a chain of data segments that represent the final WAL (Write-Ahead Log) record during WAL record insertion.

## Definition

```c
typedef struct XLogRecData
{
	struct XLogRecData *next;	/* next struct in chain, or NULL */
	char	   *data;			/* start of rmgr data to include */
	uint32		len;			/* length of rmgr data to include */
} XLogRecData;
```
## Detailed Description
The XLogRecData structure is a fundamental component of PostgreSQL's WAL record construction system. It forms a linked list that represents different data segments that need to be included in a WAL record. The functions in xloginsert.c use this structure to construct a chain of data pieces before assembling them into the final WAL record format. This design allows for efficient memory management and flexible composition of WAL records from multiple data sources without requiring immediate concatenation of all data.

## Parameters / Member Variables
- : Pointer to the next XLogRecData structure in the chain, or NULL if this is the last element
- : Pointer to the start of resource manager data that should be included in the WAL record
- : Length in bytes of the data segment pointed to by the data field

## Dependencies
- Functions called/Symbols referenced:
  - (Self-referential through next pointer)
- Called from (representative examples):
  - [XLogInsertRecord](XLogInsertRecord.md)
  - [CopyXLogRecordToWAL](../C/CopyXLogRecordToWAL.md)  
  - [XLogRecordAssemble](XLogRecordAssemble.md)
  - [XLogRegisterData](XLogRegisterData.md)
  - [XLogRegisterBufData](XLogRegisterBufData.md)
  - [XLogInsert](XLogInsert.md)

## Notes and Other Information
- This structure is primarily used internally by the WAL insertion system and is not typically manipulated directly by user code
- The linked list design allows for efficient memory usage as data segments can be added incrementally without requiring large contiguous memory allocations
- The structure is defined in xlog_internal.h, indicating it's part of the internal WAL implementation rather than the public API
- Each node in the chain represents a discrete piece of data that will be written to the WAL, allowing the system to gather data from multiple sources before final record assembly