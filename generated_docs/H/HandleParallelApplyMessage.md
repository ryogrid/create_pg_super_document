# HandleParallelApplyMessage

## Location
[src/backend/replication/logical/applyparallelworker.c:1001-1062](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L1001-L1062)

## Overview
Processes a single protocol message received from a parallel apply worker in PostgreSQL logical replication, handling error messages and providing proper error context propagation.

## Definition
```c
static void HandleParallelApplyMessage(StringInfo msg)
```

## Detailed Description
HandleParallelApplyMessage is responsible for parsing and handling messages received from parallel apply workers during logical replication. The function primarily focuses on error handling, as it processes ErrorResponse messages from workers that encounter failures. When an error is received, the function parses the error data, adds contextual information to indicate the error originated from a parallel apply worker, and re-throws the error with appropriate context. The function also handles NoticeResponse and NotifyResponse messages (though these are currently no-ops) and validates that only recognized message types are processed.

## Parameters / Member Variables
- `msg`: StringInfo containing the protocol message received from the parallel apply worker

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md)
  - [ErrorData](../E/ErrorData.md)
  - [pq_parse_errornotice](../p/pq_parse_errornotice.md)
  - errcontext
  - [psprintf](../p/psprintf.md)
  - [pstrdup](../p/pstrdup.md)
  - ereport
  - elog

- Called from (representative examples):
  - [HandleParallelApplyMessages](HandleParallelApplyMessages.md)

## Notes and Other Information
- The function uses a switch statement to handle different message types identified by a single character
- Error messages (E) are the primary focus, with proper context propagation to help identify their origin
- NoticeResponse (N) and NotifyResponse (A) are currently ignored as they are not needed for logical replication workers
- Unknown message types result in an ERROR being thrown
- The function restores the error context stack that was in effect in LogicalRepApplyLoop() for proper error handling hierarchy

## Simplified Source

```c
// Simplified version of HandleParallelApplyMessage
static void HandleParallelApplyMessage(StringInfo msg)
{
    char msgtype = pq_getmsgbyte(msg);

    switch (msgtype)
    {
        case 'E':  // ErrorResponse
        {
            ErrorData edata;

            // Parse the error message from worker
            pq_parse_errornotice(msg, &edata);

            // Add context to show this error came from parallel worker
            if (edata.context)
                edata.context = psprintf("%s\n%s", edata.context,
                                       "logical replication parallel apply worker");
            else
                edata.context = pstrdup("logical replication parallel apply worker");

            // Restore original error context stack
            error_context_stack = apply_error_context_stack;

            // Re-report the error with proper context
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("logical replication parallel apply worker exited due to error"),
                     errcontext("%s", edata.context)));
        }

        // Notice and notify messages are ignored
        case 'N':  // NoticeResponse
        case 'A':  // NotifyResponse
            break;

        default:
            elog(ERROR, "unrecognized message type from parallel worker: %c", msgtype);
    }
}
```

Key simplifications made:
- Removed detailed comments and consolidated into brief inline comments
- Simplified variable declarations by combining with initialization where possible
- Condensed the error message construction logic for clarity
- Streamlined the switch statement formatting
- Removed verbose error message details while preserving core functionality
- Maintained all essential logic: message parsing, error context handling, and proper error propagation