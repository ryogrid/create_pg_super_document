# send_repl_origin

## Location
[src/backend/replication/pgoutput/pgoutput.c:2406-2432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L2406-L2432)

## Overview
Sends a replication origin message to the logical replication subscriber when origin tracking is enabled and the origin information is available.

## Definition

```c
static void
send_repl_origin(LogicalDecodingContext *ctx, RepOriginId origin_id,
				 XLogRecPtr origin_lsn, bool send_origin)
```
## Detailed Description
This function handles the transmission of replication origin information in PostgreSQL's logical replication system. Replication origins are used to track the source of changes in multi-node replication scenarios, helping to prevent replication loops and maintain proper change provenance.

The function performs the following operations:
1. **Conditional Sending**: Only processes origin information if the send_origin flag is true
2. **Origin Resolution**: Uses replorigin_by_oid() to resolve the origin ID to a human-readable origin name
3. **Message Formatting**: If the origin name is successfully resolved, prepares and writes a logical replication origin message
4. **Error Handling**: Uses a conservative approach - if the origin name cannot be found, it silently skips sending the origin message rather than throwing an error

The function includes extensive comments discussing alternative behaviors for missing origin names, showing the PostgreSQL developers' consideration of different error handling strategies.

## Parameters / Member Variables
- `*ctx`: LogicalDecodingContext containing output plugin state and connection information
- `origin_id`: RepOriginId identifying the replication origin to send information about
- `origin_lsn`: XLogRecPtr representing the LSN of the original change at the source
- `send_origin`: Boolean flag indicating whether origin information should be sent
## Dependencies
- Functions called/Symbols referenced:
  - [replorigin_by_oid](../r/replorigin_by_oid.md) (resolve origin ID to name)
  - [OutputPluginWrite](../O/OutputPluginWrite.md) (flush pending output)
  - [OutputPluginPrepareWrite](../O/OutputPluginPrepareWrite.md) (prepare for new output)
  - [logicalrep_write_origin](../l/logicalrep_write_origin.md) (write origin message to output stream)
- Called from (representative examples):
  - [pgoutput_send_begin](../p/pgoutput_send_begin.md) (when beginning transaction with origin)
  - [pgoutput_begin_prepare_txn](../p/pgoutput_begin_prepare_txn.md) (when beginning prepared transaction with origin)
  - [pgoutput_stream_start](../p/pgoutput_stream_start.md) (when starting streaming transaction with origin)

## Notes and Other Information
- Used in multi-master and cascading replication scenarios to track change provenance
- Implements graceful degradation - missing origin names don't break replication
- The function includes detailed comments about alternative error handling strategies
- Origin messages help prevent replication loops by allowing subscribers to identify change sources
- The origin_lsn parameter preserves the original LSN from the source system
- Uses the standard OutputPlugin API for message boundaries and writing
- The conservative error handling approach prioritizes replication stability over strict origin tracking

## Simplified Source

```c
static void
send_repl_origin(LogicalDecodingContext *ctx, RepOriginId origin_id,
                 XLogRecPtr origin_lsn, bool send_origin)
{
    if (send_origin)
    {
        char *origin;

        // Try to resolve origin ID to name
        if (replorigin_by_oid(origin_id, true, &origin))
        {
            // Prepare message boundary and write origin info
            OutputPluginWrite(ctx, false);
            OutputPluginPrepareWrite(ctx, true);
            logicalrep_write_origin(ctx->out, origin, origin_lsn);
        }
        // Silently skip if origin name not found (conservative approach)
    }
}
```