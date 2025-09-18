# pqPipelineFlush

## Location
[src/interfaces/libpq/fe-exec.c:4016-4031](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L4016-L4031)

## Overview
An internal function that optimizes data transmission in pipeline mode by flushing output buffers only when they reach a threshold, while behaving like standard pqFlush in non-pipeline mode.

## Definition
```c
static int pqPipelineFlush(PGconn *conn)
```

## Detailed Description
pqPipelineFlush is an internal libpq function that implements intelligent buffer flushing behavior optimized for PostgreSQL's pipeline mode. The function provides conditional flushing logic that improves performance when sending multiple commands in sequence.

In pipeline mode, the function implements a buffering strategy where data is only flushed to the server when the output buffer size reaches the OUTBUFFER_THRESHOLD. This reduces the number of network round-trips and improves overall throughput for batch operations.

In non-pipeline mode, the function behaves identically to the standard pqFlush function, ensuring consistent behavior across different connection modes.

The threshold-based flushing in pipeline mode allows multiple small commands to be batched together in the output buffer before transmission, reducing network overhead while maintaining reasonable buffer sizes to prevent excessive memory usage.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object (PGconn pointer) - the connection whose output buffer should be conditionally flushed

## Dependencies
- Functions called/Symbols referenced:
  - PQ_PIPELINE_ON (pipeline status constant indicating active pipeline mode)
  - OUTBUFFER_THRESHOLD (buffer size threshold for triggering flush in pipeline mode)
  - [pqFlush](pqFlush.md) (underlying flush function)
- Called from (representative examples):
  - [PQsendPrepare](../P/PQsendPrepare.md) (when sending prepared statement definitions)
  - [PQsendQueryGuts](../P/PQsendQueryGuts.md) (core query sending implementation)
  - [PQsendTypedCommand](../P/PQsendTypedCommand.md) (when sending typed commands)
  - [pqPipelineSyncInternal](pqPipelineSyncInternal.md) (internal pipeline synchronization)
  - [PQsendFlushRequest](../P/PQsendFlushRequest.md) (when explicitly requesting flush)

## Notes and Other Information
- This is a static (internal) function not exposed in the public libpq API
- Returns 0 on success, following the same convention as pqFlush
- Key optimization for pipeline mode performance by reducing unnecessary network I/O
- The OUTBUFFER_THRESHOLD helps balance between throughput and memory usage
- Essential component of the pipeline mode implementation in libpq
- Automatically called by various PQsend* functions to manage buffer flushing
- The conditional logic ensures optimal behavior in both pipeline and traditional modes