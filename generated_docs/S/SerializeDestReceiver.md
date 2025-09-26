# SerializeDestReceiver

## Location
[src/backend/commands/explain.c:5314-5325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L5314-L5325)

## Overview
SerializeDestReceiver is a specialized DestReceiver implementation for PostgreSQL's EXPLAIN SERIALIZE option that serializes query result rows into RowData messages while measuring resource consumption and serialized data size without actually sending data to clients.

## Definition

```c
typedef struct SerializeDestReceiver
{
	DestReceiver pub;
	ExplainState *es;			/* this EXPLAIN statement's ExplainState */
	int8		format;			/* text or binary, like pq wire protocol */
	TupleDesc	attrinfo;		/* the output tuple desc */
	int			nattrs;			/* current number of columns */
	FmgrInfo   *finfos;			/* precomputed call info for output fns */
	MemoryContext tmpcontext;	/* per-row temporary memory context */
	StringInfoData buf;			/* buffer to hold the constructed message */
	SerializeMetrics metrics;	/* collected metrics */
} SerializeDestReceiver;
```
## Detailed Description
The SerializeDestReceiver structure extends the base DestReceiver functionality to provide a specialized destination for query tuples during EXPLAIN operations with the SERIALIZE option. Its primary purpose is to measure the overhead of deTOASTing (decompressing TOAST values) and datatype output/send functions without actually transmitting data over the network. This allows accurate performance analysis of serialization costs that are normally hidden within network transmission. The receiver processes each tuple by converting it to the wire protocol format (either text or binary), measuring the time spent and bytes generated, while collecting detailed buffer usage statistics through the embedded SerializeMetrics structure.

## Parameters / Member Variables
- `pub`: Base DestReceiver structure providing the standard interface functions for tuple reception
- `*es`: Pointer to the ExplainState structure containing configuration and state for the current EXPLAIN command
- `format`: Format specifier (int8) indicating whether to use text or binary serialization, matching PostgreSQL's wire protocol formats
- `attrinfo`: TupleDesc structure describing the schema and metadata of output tuples
- `nattrs`: Integer count of the current number of columns in the output tuple
- `*finfos`: Array of FmgrInfo structures containing precomputed function call information for output/send functions of each column's data type
- `tmpcontext`: MemoryContext for temporary allocations during per-row processing, allowing efficient memory cleanup
- `buf`: StringInfoData buffer used to construct and hold the serialized message data for each row
- `metrics`: SerializeMetrics structure accumulating performance and resource usage statistics during the serialization process
## Dependencies
- Functions called/Symbols referenced:
  - [DestReceiver](../D/DestReceiver.md) (base type)
  - [ExplainState](../E/ExplainState.md) (explain context)
  - int8 (format specification)
  - [SerializeMetrics](SerializeMetrics.md) (metrics collection)
  - [TupleDesc](../T/TupleDesc.md) (tuple descriptor)
  - [FmgrInfo](../F/FmgrInfo.md) (function manager info)
  - [MemoryContext](../M/MemoryContext.md) (memory management)
  - [StringInfoData](StringInfoData.md) (buffer management)
- Called from (representative examples):
  - [serialize_prepare_info](../s/serialize_prepare_info.md)
  - [serializeAnalyzeReceive](../s/serializeAnalyzeReceive.md)
  - [serializeAnalyzeStartup](../s/serializeAnalyzeStartup.md)
  - [serializeAnalyzeShutdown](../s/serializeAnalyzeShutdown.md)
  - [CreateExplainSerializeDestReceiver](../C/CreateExplainSerializeDestReceiver.md)
  - [GetSerializationMetrics](../G/GetSerializationMetrics.md)

## Notes and Other Information
- This struct is defined in src/backend/commands/explain.c at lines 5314-5325
- It implements a 'dry run' approach to measure serialization overhead without network transmission
- The structure allows measuring performance characteristics that are otherwise inseparable from network I/O costs
- Used exclusively with EXPLAIN commands when the SERIALIZE option is specified
- Provides detailed instrumentation for understanding the computational cost of result formatting
- The tmpcontext memory context ensures efficient cleanup of temporary allocations made during tuple processing
- Works with both text and binary protocol formats to provide comprehensive serialization measurements