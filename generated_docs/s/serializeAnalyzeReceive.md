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
  - [slot_getallattrs](slot_getallattrs.md) (tuple deconstruction)
  - [pq_beginmessage_reuse](../p/pq_beginmessage_reuse.md) (protocol message preparation)
  - [pq_sendint16](../p/pq_sendint16.md), pq_sendint32 (protocol data sending)
  - [pq_sendcountedtext](../p/pq_sendcountedtext.md), pq_sendbytes (protocol content sending)
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

## Simplified Source

```c
// Simplified version of serializeAnalyzeReceive
static bool
serializeAnalyzeReceive(TupleTableSlot *slot, DestReceiver *self)
{
    SerializeDestReceiver *myState = (SerializeDestReceiver *) self;
    TupleDesc typeinfo = slot->tts_tupleDescriptor;
    StringInfo buf = &myState->buf;
    int natts = typeinfo->natts;

    // Start timing and buffer usage measurement if enabled
    if (myState->es->timing)
        start_timing_measurement();
    if (myState->es->buffers)
        start_buffer_measurement();

    // Prepare attribute info if tuple descriptor changed
    if (myState->attrinfo != typeinfo || myState->nattrs != natts)
        serialize_prepare_info(myState, typeinfo, natts);

    // Ensure all tuple attributes are deconstructed
    slot_getallattrs(slot);

    // Switch to temporary memory context for this row
    switch_to_temp_context(myState->tmpcontext);

    // Prepare DataRow message (simulating network protocol)
    pq_beginmessage_reuse(buf, PqMsg_DataRow);
    pq_sendint16(buf, natts);

    // Process each attribute in the tuple
    for (int i = 0; i < natts; i++) {
        Datum attr = slot->tts_values[i];

        if (slot->tts_isnull[i]) {
            pq_sendint32(buf, -1);  // NULL marker
            continue;
        }

        if (myState->format == 0) {
            // Text format: convert to string and send
            char *outputstr = OutputFunctionCall(&myState->finfos[i], attr);
            pq_sendcountedtext(buf, outputstr, strlen(outputstr));
        } else {
            // Binary format: send raw bytes
            bytea *outputbytes = SendFunctionCall(&myState->finfos[i], attr);
            pq_sendint32(buf, VARSIZE(outputbytes) - VARHDRSZ);
            pq_sendbytes(buf, VARDATA(outputbytes), VARSIZE(outputbytes) - VARHDRSZ);
        }
    }

    // Count serialized bytes (don't actually send to client)
    myState->metrics.bytesSent += buf->len;

    // Clean up temporary memory and restore context
    restore_context_and_cleanup(myState->tmpcontext);

    // Record timing and buffer usage metrics
    if (myState->es->timing)
        accumulate_timing_metrics(&myState->metrics);
    if (myState->es->buffers)
        accumulate_buffer_metrics(&myState->metrics);

    return true;
}
```

Key simplifications made:
- Abstracted timing instrumentation into helper function calls
- Simplified memory context operations into descriptive function names
- Consolidated attribute processing logic with clearer variable usage
- Removed detailed protocol message handling specifics
- Abstracted buffer usage tracking into helper functions
- Maintained core logic flow: measurement setup → tuple processing → serialization → cleanup → metrics recording