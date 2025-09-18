# WaitForCommands

## Location
src/bin/pg_dump/parallel.c: 1336 - 1397

## Overview
The main routine for a worker process in pg_dump's parallel processing system that continuously reads and executes commands from the leader process until EOF is received.

## Definition


## Detailed Description
This function implements the core worker process loop in pg_dump's parallel architecture. It operates as a command interpreter that:

1. Continuously reads commands from the leader process via a pipe
2. Parses each command to extract the target table entry and action type
3. Executes the appropriate action (ACT_DUMP or ACT_RESTORE)
4. For dump operations, acquires table locks to prevent deadlocks
5. Invokes the corresponding worker function pointer for the actual work
6. Builds and sends status responses back to the leader
7. Manages memory by freeing received command strings

The function runs in an infinite loop until it receives EOF from the leader, indicating that all work has been completed and the worker should terminate. This design allows for dynamic work distribution where the leader can assign tasks to workers as they become available.

## Parameters / Member Variables
- : Archive handle containing database connection, function pointers (WorkerJobDumpPtr, WorkerJobRestorePtr), and other dump/restore context
- : Two-element array representing the pipe file descriptors for communication with the leader process

## Dependencies
- Functions called/Symbols referenced:
  - getMessageFromLeader (receives commands from leader process)
  - parseWorkerCommand (decodes command into table entry and action)
  - lockTableForWorker (acquires table locks for dump operations)
  - buildWorkerResponse (creates status response message)
  - sendMessageToLeader (sends response back to leader)
  - ACT_DUMP, ACT_RESTORE (action type constants)
  - TocEntry, T_Action (data structures)

- Called from (representative examples):
  - RunWorker (worker process initialization and execution)

## Notes and Other Information
- Central component of pg_dump's parallel processing infrastructure
- Handles both dump and restore operations in worker processes
- Implements proper memory management by freeing received command strings
- Uses function pointers (WorkerJobDumpPtr, WorkerJobRestorePtr) for operation-specific logic
- Ensures table locking only for dump operations to prevent deadlocks
- Communicates status back to leader for each completed operation
- Terminates gracefully when leader closes the communication pipe