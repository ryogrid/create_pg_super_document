# ListenToWorkers

## Location
src/bin/pg_dump/parallel.c: 1398 - 1450

## Overview
Collects status messages from worker processes in pg_dump's parallel processing system and processes completion callbacks to manage worker state transitions.

## Definition


## Detailed Description
This function serves as the communication hub between the leader process and worker processes in pg_dump's parallel architecture. It handles incoming status messages from workers and manages the lifecycle of parallel operations by:

1. Attempting to collect status messages from any available worker
2. Optionally waiting for messages based on the do_wait parameter
3. Parsing and validating received messages (expecting "OK" prefix)
4. Executing completion callbacks that were registered when jobs were dispatched
5. Updating worker status from busy back to IDLE state
6. Cleaning up job-specific state and freeing memory

The function supports both blocking and non-blocking operation modes. In blocking mode (do_wait=true), it waits for a worker to complete and treats EOF as a fatal error indicating worker death. In non-blocking mode, it returns immediately if no messages are available.

The callback mechanism allows different types of operations (dump, restore, etc.) to have custom completion handling while maintaining a unified worker management interface.

## Parameters / Member Variables
- : Archive handle containing the database connection and operation context
- : Parallel state structure containing worker slots, job assignments, and communication infrastructure
- : Boolean flag controlling whether to wait for messages (true) or return immediately if none available (false)

## Dependencies
- Functions called/Symbols referenced:
  - [getMessageFromWorker](../g/getMessageFromWorker.md) (retrieves status messages from worker processes)
  - messageStartsWith (validates message format with "OK" prefix)
  - [parseWorkerResponse](../p/parseWorkerResponse.md) (extracts status code from worker response)
  - [callback](../c/callback.md) (user-defined completion handler function)
  - [ParallelState](../P/ParallelState.md), ParallelSlot, TocEntry (data structures)
  - WRKR_IDLE (worker status constant)

- Called from (representative examples):
  - [WaitForWorkers](../W/WaitForWorkers.md) (main coordination loop for parallel operations)

## Notes and Other Information
- Returns true if a status message was successfully collected and processed
- Handles worker death detection when do_wait is true and EOF is encountered
- Validates message format and treats invalid messages as fatal errors
- Manages memory by freeing received message strings
- Updates parallel state to mark workers as available for new assignments
- Central component for coordinating completion of parallel dump/restore operations
- Comment suggests potential optimization to collect multiple messages per call