# HandleParallelMessage

## Location
[src/backend/access/transam/parallel.c:1133-1249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L1133-L1249)

## Overview
Processes a single protocol message received from a specific parallel worker, handling various message types including errors, notices, notifications, progress updates, and termination signals.

## Definition

```c
static void
HandleParallelMessage(ParallelContext *pcxt, int i, StringInfo msg)
```
## Detailed Description
This static function is the core message dispatcher for individual parallel worker messages. It parses and handles different types of protocol messages sent from parallel workers to the main backend process. The function first tracks worker attachment status, then switches on the message type to perform appropriate handling.

The function handles five main message types:

1. **PqMsg_ErrorResponse/PqMsg_NoticeResponse**: Parses error or notice messages from workers, adjusts severity levels (capping at ERROR to prevent main process suicide), adds parallel worker context information, and re-throws errors or prints notices using the original error context stack.

2. **PqMsg_NotificationResponse**: Forwards NOTIFY messages from parallel workers to the frontend client by extracting the PID, channel, and payload, then calling NotifyMyFrontEnd().

3. **PqMsg_Progress**: Handles incremental progress reporting from parallel workers by extracting index and increment values and updating progress statistics via pgstat_progress_incr_param().

4. **PqMsg_Terminate**: Handles worker termination by detaching from the worker's error message queue and setting the handle to NULL.

5. **Unknown message types**: Reports an error for unrecognized message types.

## Parameters / Member Variables
- `*pcxt`: Pointer to the ParallelContext containing information about the parallel execution environment
- `i`: Index of the specific worker that sent the message (0-based)
- `msg`: StringInfo containing the raw message data received from the worker
## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md)
  - [pq_parse_errornotice](../p/pq_parse_errornotice.md)  
  - [ThrowErrorData](../T/ThrowErrorData.md)
  - [pq_getmsgint](../p/pq_getmsgint.md)
  - [pq_getmsgrawstring](../p/pq_getmsgrawstring.md)
  - [pq_endmessage](../p/pq_endmessage.md)
  - [pq_getmsgint64](../p/pq_getmsgint64.md)
  - [pq_getmsgend](../p/pq_getmsgend.md)
  - [NotifyMyFrontEnd](../N/NotifyMyFrontEnd.md)
  - [pgstat_progress_incr_param](../p/pgstat_progress_incr_param.md)
  - [shm_mq_detach](../s/shm_mq_detach.md)
  - [psprintf](../p/psprintf.md)
  - [pstrdup](../p/pstrdup.md)
  - Min
  - elog

- Called from (representative examples):
  - [HandleParallelMessages](HandleParallelMessages.md)

## Notes and Other Information
- This is a static function only called from HandleParallelMessages()
- Worker attachment tracking helps determine which workers have successfully connected
- Error level capping prevents parallel worker errors from terminating the main process
- Context information is added to errors to clarify they originated from parallel workers (except in DEBUG_PARALLEL_REGRESS mode)
- The original error context stack from ParallelContext creation is preserved for proper error reporting
- Progress reporting only supports incremental updates currently but is designed for extensibility
- Proper cleanup occurs when workers terminate by detaching message queue handles

## Simplified Source

```c
// Simplified version of HandleParallelMessage
static void
HandleParallelMessage(ParallelContext *pcxt, int worker_index, StringInfo msg)
{
    char msgtype;

    // Track worker attachment status
    if (pcxt->known_attached_workers != NULL &&
        !pcxt->known_attached_workers[worker_index]) {
        pcxt->known_attached_workers[worker_index] = true;
        pcxt->nknown_attached_workers++;
    }

    // Extract message type and dispatch
    msgtype = pq_getmsgbyte(msg);

    switch (msgtype) {
        case PqMsg_ErrorResponse:
        case PqMsg_NoticeResponse:
            {
                ErrorData edata;

                // Parse error/notice from worker
                pq_parse_errornotice(msg, &edata);

                // Cap error level to prevent main process termination
                edata.elevel = Min(edata.elevel, ERROR);

                // Add parallel worker context for clarity
                if (debug_parallel_query != DEBUG_PARALLEL_REGRESS) {
                    if (edata.context)
                        edata.context = psprintf("%s\n%s", edata.context, "parallel worker");
                    else
                        edata.context = pstrdup("parallel worker");
                }

                // Use original error context and re-throw
                ErrorContextCallback *saved_context = error_context_stack;
                error_context_stack = pcxt->error_context_stack;
                ThrowErrorData(&edata);
                error_context_stack = saved_context;

                break;
            }

        case PqMsg_NotificationResponse:
            {
                // Forward NOTIFY from worker to frontend
                int32 pid = pq_getmsgint(msg, 4);
                const char *channel = pq_getmsgrawstring(msg);
                const char *payload = pq_getmsgrawstring(msg);
                pq_endmessage(msg);

                NotifyMyFrontEnd(channel, payload, pid);
                break;
            }

        case PqMsg_Progress:
            {
                // Handle incremental progress updates
                int index = pq_getmsgint(msg, 4);
                int64 increment = pq_getmsgint64(msg);
                pq_getmsgend(msg);

                pgstat_progress_incr_param(index, increment);
                break;
            }

        case PqMsg_Terminate:
            {
                // Clean up worker's error message queue
                shm_mq_detach(pcxt->worker[worker_index].error_mqh);
                pcxt->worker[worker_index].error_mqh = NULL;
                break;
            }

        default:
            elog(ERROR, "unrecognized message type from parallel worker: %c", msgtype);
    }
}
```

Key simplifications made:
- Renamed parameter `i` to `worker_index` for clarity
- Added descriptive comments for each major logic block
- Consolidated error context handling into a cleaner flow
- Simplified variable declarations and removed unnecessary braces
- Focused on the core message dispatching logic
- Preserved all essential functionality while improving readability