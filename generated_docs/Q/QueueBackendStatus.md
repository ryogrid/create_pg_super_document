# QueueBackendStatus

## Location
src/backend/commands/async.c: 243 - 249

## Overview
QueueBackendStatus is a structure that tracks the status and queue reading position of a backend process listening for asynchronous notifications.

## Definition


## Detailed Description
The QueueBackendStatus structure maintains the state of a backend process that is participating in PostgreSQL's asynchronous notification system. It tracks which backend process is listening (identified by PID and database), maintains a linked list of listeners through the nextListener field, and critically tracks how far through the notification queue this particular backend has read. This allows the system to manage multiple listeners efficiently, ensuring that notifications are not discarded until all interested backends have processed them.

## Parameters / Member Variables
- : Process ID of the listening backend, or InvalidPid if the slot is unused
- : Database OID that the backend is connected to, or InvalidOid if invalid
- : Process number of the next listener in a linked list structure, or INVALID_PROC_NUMBER if this is the last listener
- : QueuePosition indicating how far through the notification queue this backend has read

## Dependencies
- Functions called/Symbols referenced:
  - ProcNumber
  - [QueuePosition](QueuePosition.md)
- Called from (representative examples):
  - [AsyncQueueControl](../A/AsyncQueueControl.md)
  - [AsyncShmemSize](../A/AsyncShmemSize.md)
  - [AsyncShmemInit](../A/AsyncShmemInit.md)

## Notes and Other Information
- Part of PostgreSQL's LISTEN/NOTIFY mechanism for inter-backend communication
- Forms a linked list structure through the nextListener field to efficiently manage multiple listening backends
- The pos field is crucial for garbage collection - the system can only discard notification queue entries after all backends have read past that position
- Used in shared memory structures to coordinate between multiple PostgreSQL backend processes
- Invalid values (InvalidPid, InvalidOid, INVALID_PROC_NUMBER) are used to indicate unused or invalid states