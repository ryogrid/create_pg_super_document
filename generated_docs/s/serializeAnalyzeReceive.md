# serializeAnalyzeReceive

## Location
[src/backend/commands/explain.c:5387-5489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L5387-L5489)

## Overview
A callback function that collects and measures tuples for EXPLAIN (SERIALIZE) operations, tracking performance metrics while simulating tuple serialization.

## Definition
static bool serializeAnalyzeReceive(TupleTableSlot *slot, DestReceiver *self)

## Detailed Description
This function serves as the receive callback for EXPLAIN (SERIALIZE) operations, designed to closely match printtup() from printtup.c but with added measurement capabilities. It processes each tuple by preparing it for wire protocol transmission (either text or binary format), while collecting timing and buffer usage metrics without actually sending data to the client. The function handles tuple deconstruction, format conversion, message preparation, and resource cleanup, all while maintaining accurate performance measurements for analysis purposes.

## Parameters / Member Variables
- slot: TupleTableSlot containing the tuple data to be processed
- self: DestReceiver pointer (cast to SerializeDestReceiver) containing state and metrics

## Dependencies
- Functions called/Symbols referenced:
  - [serialize_prepare_info](serialize_prepare_info.md) (function info preparation)
  - slot_getallattrs (tuple deconstruction)
  - pq_beginmessage_reuse (protocol message preparation)
  - [pq_sendint16](../p/pq_sendint16.md), pq_sendint32 (protocol data sending)
  - pq_sendcountedtext, pq_sendbytes (protocol content sending)
  - [OutputFunctionCall](../O/OutputFunctionCall.md), SendFunctionCall (type output functions)
  - INSTR_TIME_SET_CURRENT, INSTR_TIME_ACCUM_DIFF (timing instrumentation)
  - [BufferUsageAccumDiff](../B/BufferUsageAccumDiff.md) (buffer usage tracking)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md), MemoryContextReset (memory management)
- Called from (representative examples):
  - [CreateExplainSerializeDestReceiver](../C/CreateExplainSerializeDestReceiver.md) (as callback function)

## Notes and Other Information
- This is a static function only accessible within the explain.c file
- Supports both text (format 0) and binary (format 1) wire protocol formats
- Collects timing metrics only when timing is enabled in ExplainState
- Tracks buffer usage statistics only when buffer tracking is enabled
- Uses per-row memory context to manage temporary allocations efficiently
- Part of PostgreSQL's EXPLAIN (SERIALIZE) functionality for performance analysis
- Returns true to indicate successful processing of the tuple