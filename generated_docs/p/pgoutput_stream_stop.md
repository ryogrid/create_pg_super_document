# pgoutput_stream_stop

## Location
[src/backend/replication/pgoutput/pgoutput.c:1815-1835](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1815-L1835)

## Overview
pgoutput_stream_stop is a callback function that handles the end of streaming for large transactions in PostgreSQL logical replication, outputting stream stop messages to the replication protocol.

## Definition

```c
static void
pgoutput_stream_stop(struct LogicalDecodingContext *ctx,
					 ReorderBufferTXN *txn)
```
## Detailed Description
pgoutput_stream_stop is a callback function in the pgoutput logical replication output plugin that handles the STOP STREAM event for large transactions that are being streamed in chunks. When a stream chunk of a large transaction completes, this function is called to signal the end of that particular stream. It writes a stream stop message to the logical replication protocol and updates the internal state to indicate that streaming has stopped for this chunk. The function includes an assertion to ensure that streaming was actually active before attempting to stop it.

## Parameters / Member Variables
- : Logical decoding context containing output plugin state and configuration
- : ReorderBufferTXN structure representing the transaction whose stream is ending

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md)
  - [PGOutputData](../P/PGOutputData.md)
  - [OutputPluginPrepareWrite](../O/OutputPluginPrepareWrite.md)
  - [logicalrep_write_stream_stop](../l/logicalrep_write_stream_stop.md)
  - [OutputPluginWrite](../O/OutputPluginWrite.md)
- Called from (representative examples):
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (registered as callback)

## Notes and Other Information
- This is a static function, only accessible within the pgoutput.c file
- Part of the streaming transaction feature that complements pgoutput_stream_start
- Includes an assertion to ensure that streaming was active (in_streaming flag was true)
- Sets the in_streaming flag in PGOutputData to false to indicate streaming has stopped
- Uses the logical replication protocol message format for communicating with subscribers
- Critical for proper transaction streaming lifecycle management in logical replication
- Works in conjunction with pgoutput_stream_start and pgoutput_stream_abort to handle transaction streaming