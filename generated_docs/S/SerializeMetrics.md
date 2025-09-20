# SerializeMetrics

## Location
[src/backend/commands/explain.c:53-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L53-L58)

## Overview
SerializeMetrics is a struct that tracks instrumentation data for PostgreSQL's SERIALIZE option in EXPLAIN commands, capturing performance metrics during result serialization.

## Definition

```c
typedef struct SerializeMetrics
{
	uint64		bytesSent;		/* # of bytes serialized */
	instr_time	timeSpent;		/* time spent serializing */
	BufferUsage bufferUsage;	/* buffers accessed during serialization */
} SerializeMetrics;
```
## Detailed Description
The SerializeMetrics structure is used to collect and store performance instrumentation data when PostgreSQL executes queries with the SERIALIZE option enabled in EXPLAIN commands. This struct provides comprehensive metrics about the serialization process, including the volume of data serialized, the time taken for serialization, and detailed buffer usage statistics. It serves as a container for monitoring the efficiency and resource consumption of the result serialization phase, which is crucial for performance analysis and optimization of query execution plans.

## Parameters / Member Variables
- `bytesSent`: A 64-bit unsigned integer that tracks the total number of bytes that have been serialized during the operation
- `timeSpent`: An instr_time structure that measures the total time spent in the serialization process, stored in platform-specific time units
- `bufferUsage`: A BufferUsage structure that contains detailed statistics about buffer access patterns during serialization, including shared buffer hits/reads, local buffer operations, temporary block operations, and associated timing information
## Dependencies
- Functions called/Symbols referenced:
  - [instr_time](../i/instr_time.md) (time measurement type)
  - BufferUsage (buffer access statistics type)
- Called from (representative examples):
  - [ExplainOnePlan](../E/ExplainOnePlan.md)
  - ExplainPrintSerialize
  - [SerializeDestReceiver](SerializeDestReceiver.md)
  - [serializeAnalyzeStartup](../s/serializeAnalyzeStartup.md)
  - [CreateExplainSerializeDestReceiver](../C/CreateExplainSerializeDestReceiver.md)
  - [GetSerializationMetrics](../G/GetSerializationMetrics.md)

## Notes and Other Information
- This struct is defined in src/backend/commands/explain.c at lines 53-58
- It's specifically used for instrumentation when the SERIALIZE option is enabled in EXPLAIN commands
- The metrics collected help in understanding the performance characteristics of result serialization
- The struct integrates timing information (instr_time) with buffer usage statistics (BufferUsage) to provide comprehensive serialization metrics
- Used in conjunction with BYTES_TO_KILOBYTES macro for formatting output in explain plans