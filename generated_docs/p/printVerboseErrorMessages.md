# printVerboseErrorMessages

## Location
[src/bin/pgbench/pgbench.c:3564-3601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L3564-L3601)

## Overview
Prints detailed verbose error messages for failed transactions in pgbench, providing diagnostic information about retry attempts and performance metrics.

## Definition
static void printVerboseErrorMessages(CState *st, pg_time_usec_t *now, bool is_retry)

## Detailed Description
This function generates comprehensive error messages when transactions fail in pgbench. It provides detailed diagnostic information including client ID, retry status, attempt counts, and performance metrics. The function uses a static PQExpBuffer to efficiently build formatted messages that help users understand transaction failure patterns and retry behavior.

Key features include:
- Client identification for multi-client scenarios
- Clear indication of retry vs. termination behavior
- Try count reporting with optional maximum try limits
- Latency percentage calculation when latency limits are configured
- Efficient message buffer management using static allocation

## Parameters / Member Variables
- : Pointer to CState structure containing client state information (ID, try counts, transaction scheduling time)
- : Pointer to pg_time_usec_t timestamp, updated lazily if latency limit reporting is needed
- : Boolean flag indicating whether this is a retry attempt (true) or transaction termination (false)

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - [printfPQExpBuffer](printfPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [pg_time_now_lazy](pg_time_now_lazy.md)
  - pg_log_info
- Global variables referenced:
  - max_tries
  - latency_limit
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md)

## Notes and Other Information
- Uses a static PQExpBuffer for efficient string building across multiple calls
- Conditionally includes maximum try count only when max_tries is set (not unlimited)
- Calculates and reports latency percentage only when latency_limit is configured
- Provides different messages for retry attempts vs. transaction termination
- Essential for debugging and monitoring pgbench performance in verbose mode
- Messages are logged at INFO level for visibility without being overly intrusive

## Simplified Source

```c
static void printVerboseErrorMessages(CState *st, pg_time_usec_t *now, bool is_retry)
{
    static PQExpBuffer buf = NULL;

    // Initialize or reset message buffer
    if (buf == NULL)
        buf = createPQExpBuffer();
    else
        resetPQExpBuffer(buf);

    // Build basic message with client ID and retry status
    printfPQExpBuffer(buf, "client %d ", st->id);
    appendPQExpBufferStr(buf, is_retry ?
                         "repeats the transaction after the error" :
                         "ends the failed transaction");
    appendPQExpBuffer(buf, " (try %u", st->tries);

    // Add max tries if not unlimited
    if (max_tries)
        appendPQExpBuffer(buf, "/%u", max_tries);

    // Add latency percentage if latency limit is configured
    if (latency_limit)
    {
        pg_time_now_lazy(now);
        appendPQExpBuffer(buf, ", %.3f%% of the maximum time of tries was used",
                          (100.0 * (*now - st->txn_scheduled) / latency_limit));
    }

    appendPQExpBufferStr(buf, ")\n");

    // Output the formatted message
    pg_log_info("%s", buf->data);
}
```