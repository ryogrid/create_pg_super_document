# getMessageFromLeader

## Location
[src/bin/pg_dump/parallel.c:1516-1526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L1516-L1526)

## Overview
Reads command messages from the leader process in pg_dump's parallel processing system, providing the communication interface for worker processes.

## Definition

```c
static char *
getMessageFromLeader(int pipefd[2])
```
## Detailed Description
This function serves as a simple wrapper around the lower-level pipe reading functionality, specifically designed for worker processes to receive commands from the leader process. It provides a blocking read operation that:

1. Waits for command messages to arrive from the leader via the designated read pipe
2. Returns the received message as a dynamically allocated string that the caller must free
3. Returns NULL when EOF is encountered, signaling that the leader has finished sending commands

The function abstracts the pipe communication details and provides a clean interface for the worker's command processing loop. It specifically uses the read end of the pipe (PIPE_READ) from the pipe pair established during worker process creation.

As a blocking operation, this function will suspend the worker process until a message arrives or the pipe is closed, making it suitable for the worker's main command processing loop where it should wait for instructions from the leader.

## Parameters / Member Variables
- `pipefd[2]`: Two-element array representing the pipe file descriptors for communication with the leader process, where pipefd[PIPE_READ] is used for reading commands
## Dependencies
- Functions called/Symbols referenced:
  - [readMessageFromPipe](../r/readMessageFromPipe.md) (low-level pipe reading function that handles message framing and memory allocation)
  - PIPE_READ (constant defining the read end index of the pipe descriptor array)

- Called from (representative examples):
  - [WaitForCommands](../W/WaitForCommands.md) (main worker command processing loop)

## Notes and Other Information
- Executed exclusively in worker processes as part of the parallel processing architecture
- Returns dynamically allocated memory that must be freed by the caller
- Provides blocking semantics - will wait indefinitely for messages
- EOF return (NULL) indicates normal termination when leader closes communication
- Simple wrapper that encapsulates pipe communication details for command reading
- Part of the bidirectional communication system between leader and worker processes

## Simplified Source

```c
static char *getMessageFromLeader(int pipefd[2]) {
    // Simple wrapper around pipe reading - blocks until message available
    return readMessageFromPipe(pipefd[PIPE_READ]);
}
```