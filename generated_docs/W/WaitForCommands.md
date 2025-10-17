# WaitForCommands

## Location
[src/bin/pg_dump/parallel.c:1336-1397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L1336-L1397)

## Overview
The main routine for a worker process in pg_dump's parallel processing system that continuously reads and executes commands from the leader process until EOF is received.

## Definition

```c
static void
WaitForCommands(ArchiveHandle *AH, int pipefd[2])
```
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
- `*AH`: Archive handle containing database connection, function pointers (WorkerJobDumpPtr, WorkerJobRestorePtr), and other dump/restore context
- `pipefd[2]`: Two-element array representing the pipe file descriptors for communication with the leader process
## Dependencies
- Functions called/Symbols referenced:
  - [getMessageFromLeader](../g/getMessageFromLeader.md) (receives commands from leader process)
  - [parseWorkerCommand](../p/parseWorkerCommand.md) (decodes command into table entry and action)
  - [lockTableForWorker](../l/lockTableForWorker.md) (acquires table locks for dump operations)
  - [buildWorkerResponse](../b/buildWorkerResponse.md) (creates status response message)
  - [sendMessageToLeader](../s/sendMessageToLeader.md) (sends response back to leader)
  - ACT_DUMP, ACT_RESTORE (action type constants)
  - [TocEntry](../T/TocEntry.md), T_Action (data structures)

- Called from (representative examples):
  - [RunWorker](../R/RunWorker.md) (worker process initialization and execution)

## Notes and Other Information
- Central component of pg_dump's parallel processing infrastructure
- Handles both dump and restore operations in worker processes
- Implements proper memory management by freeing received command strings
- Uses function pointers (WorkerJobDumpPtr, WorkerJobRestorePtr) for operation-specific logic
- Ensures table locking only for dump operations to prevent deadlocks
- Communicates status back to leader for each completed operation
- Terminates gracefully when leader closes the communication pipe

## Simplified Source

```c
static void WaitForCommands(ArchiveHandle *AH, int pipefd[2]) {
    char *command;
    TocEntry *te;
    T_Action act;
    int status = 0;
    char buf[256];

    // Main worker loop - process commands until EOF
    for (;;) {
        // Get command from leader (blocks until available)
        command = getMessageFromLeader(pipefd);
        if (!command) {
            return;  // EOF - leader finished sending commands
        }

        // Parse the command to get table entry and action
        parseWorkerCommand(AH, &te, &act, command);

        // Execute the appropriate action
        if (act == ACT_DUMP) {
            // Acquire table lock before dumping to prevent deadlocks
            lockTableForWorker(AH, te);
            status = (AH->WorkerJobDumpPtr)(AH, te);
        } else if (act == ACT_RESTORE) {
            status = (AH->WorkerJobRestorePtr)(AH, te);
        } else {
            Assert(false);  // Unknown action type
        }

        // Send status back to leader
        buildWorkerResponse(AH, te, act, status, buf, sizeof(buf));
        sendMessageToLeader(pipefd, buf);

        // Free the command string
        free(command);
    }
}
```