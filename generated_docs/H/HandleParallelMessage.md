# HandleParallelMessage

## Location
src/backend/access/transam/parallel.c: 1133 - 1249

## Overview
Processes a single protocol message received from a specific parallel worker, handling various message types including errors, notices, notifications, progress updates, and termination signals.

## Definition


## Detailed Description
This static function is the core message dispatcher for individual parallel worker messages. It parses and handles different types of protocol messages sent from parallel workers to the main backend process. The function first tracks worker attachment status, then switches on the message type to perform appropriate handling.

The function handles five main message types:

1. **PqMsg_ErrorResponse/PqMsg_NoticeResponse**: Parses error or notice messages from workers, adjusts severity levels (capping at ERROR to prevent main process suicide), adds parallel worker context information, and re-throws errors or prints notices using the original error context stack.

2. **PqMsg_NotificationResponse**: Forwards NOTIFY messages from parallel workers to the frontend client by extracting the PID, channel, and payload, then calling NotifyMyFrontEnd().

3. **PqMsg_Progress**: Handles incremental progress reporting from parallel workers by extracting index and increment values and updating progress statistics via pgstat_progress_incr_param().

4. **PqMsg_Terminate**: Handles worker termination by detaching from the worker's error message queue and setting the handle to NULL.

5. **Unknown message types**: Reports an error for unrecognized message types.

## Parameters / Member Variables
- : Pointer to the ParallelContext containing information about the parallel execution environment
- : Index of the specific worker that sent the message (0-based)
- : StringInfo containing the raw message data received from the worker

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