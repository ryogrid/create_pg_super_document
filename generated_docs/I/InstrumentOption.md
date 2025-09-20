# InstrumentOption

## Location
[src/include/executor/instrument.h:66-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/instrument.h#L66-L67)

## Overview
InstrumentOption is an enumeration that defines flag bits used in the InstrAlloc function's instrument_options bitmask to configure what types of execution statistics and metrics should be collected during query execution.

## Definition

```c
typedef struct Instrumentation
{
	/* Parameters set at node creation: */
	bool		need_timer;		/* true if we need timer data */
	bool		need_bufusage;	/* true if we need buffer usage data */
	bool		need_walusage;	/* true if we need WAL usage data */
	bool		async_mode;		/* true if node is in async mode */
	/* Info about current plan cycle: */
	bool		running;		/* true if we've completed first tuple */
	instr_time	starttime;		/* start time of current iteration of node */
	instr_time	counter;		/* accumulated runtime for this node */
	double		firsttuple;		/* time for first tuple of this cycle */
	double		tuplecount;		/* # of tuples emitted so far this cycle */
	BufferUsage bufusage_start; /* buffer usage at start */
	WalUsage	walusage_start; /* WAL usage at start */
	/* Accumulated statistics across all completed cycles: */
	double		startup;		/* total startup time (in seconds) */
	double		total;			/* total time (in seconds) */
	double		ntuples;		/* total tuples produced */
	double		ntuples2;		/* secondary node-specific tuple counter */
	double		nloops;			/* # of run cycles for this node */
	double		nfiltered1;		/* # of tuples removed by scanqual or joinqual */
	double		nfiltered2;		/* # of tuples removed by "other" quals */
	BufferUsage bufusage;		/* total buffer usage */
	WalUsage	walusage;		/* total WAL usage */
} Instrumentation;
```
## Detailed Description
InstrumentOption provides a set of bitwise flags that control which types of performance metrics and statistics are collected during PostgreSQL query execution. These flags are combined using bitwise OR operations to create a bitmask that specifies the desired instrumentation level. The enum is designed to allow fine-grained control over what execution statistics are gathered, enabling users to balance performance monitoring needs with execution overhead.

## Parameters / Member Variables
- `INSTRUMENT_TIMER`: Enables timing instrumentation and row count collection for execution nodes
- `INSTRUMENT_BUFFERS`: Enables buffer usage statistics collection (I/O operations, cache hits/misses)  
- `INSTRUMENT_ROWS`: Enables row count statistics collection independently of timing
- `INSTRUMENT_WAL`: Enables Write-Ahead Log usage statistics collection
- `INSTRUMENT_ALL`: Special value (PG_INT32_MAX) that enables all available instrumentation options

## Dependencies
- Functions called/Symbols referenced:
  - Used as parameter type in InstrAlloc function
  - Referenced in explain.c for EXPLAIN command instrumentation options
- Called from (representative examples):
  - InstrAlloc (src/backend/executor/instrument.c:29)
  - explain.c instrumentation setup (src/backend/commands/explain.c:634-641)

## Notes and Other Information
- The enum values use bit shifting to create distinct bit flags that can be combined using bitwise OR operations
- These flags correspond to different performance aspects: execution timing, memory/buffer usage, row processing counts, and WAL generation
- The instrumentation system is used extensively by PostgreSQL's EXPLAIN functionality to provide detailed query execution statistics
- INSTRUMENT_ALL serves as a convenience flag to enable comprehensive instrumentation across all categories
- The flags directly influence which fields are populated in the Instrumentation structure during query execution