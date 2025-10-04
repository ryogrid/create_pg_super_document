# pg_listening_channels

## Location
[src/backend/commands/async.c:790-822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L790-L822)

## Overview
A SQL function that returns a set of channel names that the current backend process is actively listening to via LISTEN commands.

## Definition

```c
Datum
pg_listening_channels(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL SQL function  that allows users to query which notification channels the current database session is listening to. It returns a set-returning function (SRF) that iterates through the global  list and returns each channel name as a text datum.

The function uses the standard PostgreSQL SRF framework with  for initialization and  for each subsequent call. It relies on the fact that the listen channels list cannot change within a transaction, ensuring consistent results during the function execution.

## Parameters / Member Variables
- Returns:  - PostgreSQL internal data type representing the result set

## Dependencies
- Functions called/Symbols referenced:
  -  - PostgreSQL SRF context structure
  -  - Macro to check if this is the first call
  -  - [Initialize](../I/Initialize.md) SRF context
  -  - Setup for each call
  -  - Get length of PostgreSQL List
  -  - Get nth element from PostgreSQL List
  -  - Convert C string to PostgreSQL text datum
  -  - Return next value in set
  -  - Signal end of set
  -  - Global list of channels being listened to
- Called from:
  - SQL queries using 

## Notes and Other Information
- This function provides introspection capability for the LISTEN/NOTIFY system
- The function is read-only and does not modify the listen state
- Results are guaranteed to be consistent within a transaction due to the immutable nature of the listen channels list during transaction execution
- Location: src/backend/commands/async.c:790-822

## Simplified Source

```c
Datum
pg_listening_channels(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;

    // Initialize on first call
    if (SRF_IS_FIRSTCALL())
    {
        funcctx = SRF_FIRSTCALL_INIT();
    }

    // Setup for each call
    funcctx = SRF_PERCALL_SETUP();

    // Return next channel name if available
    if (funcctx->call_cntr < list_length(listenChannels))
    {
        char *channel = (char *) list_nth(listenChannels, funcctx->call_cntr);
        SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(channel));
    }

    // No more channels to return
    SRF_RETURN_DONE(funcctx);
}
```