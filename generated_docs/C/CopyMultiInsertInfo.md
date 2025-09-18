# CopyMultiInsertInfo

## Location
src/backend/commands/copyfrom.c: 91 - 100

## Overview
CopyMultiInsertInfo is a structure that manages one or multiple CopyMultiInsertBuffer instances, tracking the overall state and statistics of buffered tuples across all buffers during COPY FROM operations, particularly useful for partitioned tables.

## Definition
```c
typedef struct CopyMultiInsertInfo
{
	List	   *multiInsertBuffers; /* List of tracked CopyMultiInsertBuffers */
	int			bufferedTuples; /* number of tuples buffered over all buffers */
	int			bufferedBytes;	/* number of bytes from all buffered tuples */
	CopyFromState cstate;		/* Copy state for this CopyMultiInsertInfo */
	EState	   *estate;			/* Executor state used for COPY */
	CommandId	mycid;			/* Command Id used for COPY */
	int			ti_options;		/* table insert options */
} CopyMultiInsertInfo;
```

## Detailed Description
CopyMultiInsertInfo serves as a higher-level coordinator for multi-insert buffering during COPY FROM operations. It maintains a list of CopyMultiInsertBuffer instances, which is especially important when copying data into partitioned tables where different partitions may require separate buffers. The structure tracks aggregate statistics across all buffers, including the total number of buffered tuples and bytes, enabling efficient memory management and flush decisions.

The structure integrates with PostgreSQL's executor framework through the EState pointer and maintains necessary context information for the COPY operation, including command ID for transaction isolation and table insert options for proper tuple handling.

## Parameters / Member Variables
- `multiInsertBuffers`: List containing pointers to CopyMultiInsertBuffer structures, one per target relation/partition
- `bufferedTuples`: Running count of total tuples currently buffered across all CopyMultiInsertBuffer instances
- `bufferedBytes`: Total size in bytes of all buffered tuple data across all buffers
- `cstate`: Copy state structure containing the current context and configuration for the COPY FROM operation
- `estate`: Executor state pointer providing access to execution context and memory management
- `mycid`: Command identifier used for transaction visibility and isolation during the COPY operation
- `ti_options`: Table insert options flags controlling insertion behavior

## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (PostgreSQL list structure)
  - CopyFromState
  - [EState](../E/EState.md)
  - CommandId
  - [CopyMultiInsertBuffer](CopyMultiInsertBuffer.md) (via multiInsertBuffers list)
- Called from (representative examples):
  - [CopyMultiInsertInfoInit](CopyMultiInsertInfoInit.md)
  - [CopyMultiInsertInfoFlush](CopyMultiInsertInfoFlush.md)
  - [CopyMultiInsertInfoCleanup](CopyMultiInsertInfoCleanup.md)
  - [CopyMultiInsertInfoSetupBuffer](CopyMultiInsertInfoSetupBuffer.md)
  - [CopyFrom](CopyFrom.md)

## Notes and Other Information
- Essential for managing COPY operations into partitioned tables where multiple CopyMultiInsertBuffer instances are needed
- Provides centralized tracking of buffer statistics to make intelligent decisions about when to flush buffers
- The bufferedBytes tracking helps with memory management and prevents excessive memory usage
- Works closely with the PostgreSQL executor framework through the EState integration
- [Command](Command.md) ID tracking ensures proper transaction isolation during bulk insert operations
- Designed to handle complex scenarios where data needs to be distributed across multiple target relations or partitions