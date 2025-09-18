# copy_dest_shutdown

## Location
[src/backend/commands/copyto.c:1254-1262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyto.c#L1254-L1262)

## Overview
A no-operation shutdown function for the COPY destination receiver that implements the DestReceiver interface's shutdown callback.

## Definition


## Detailed Description
This function serves as the shutdown callback for the COPY destination receiver in PostgreSQL's executor framework. It implements the DestReceiver interface requirement for a shutdown function but performs no operations (no-op). The function is called by the executor when completing or terminating COPY operations, but since COPY destination receivers handle all cleanup operations through their destroy callback rather than the shutdown callback, this function remains empty. The actual cleanup of COPY state, file handles, and resources occurs in the copy_dest_destroy function.

## Parameters / Member Variables
- : Pointer to the DestReceiver structure representing the COPY destination receiver

## Dependencies
- Functions called/Symbols referenced:
  -  (interface structure)
- Called from (representative examples):
  -  (during receiver setup as callback assignment)

## Notes and Other Information
- This is a callback function that gets assigned to the DestReceiver's rShutdown field during COPY destination receiver initialization
- The no-op implementation indicates that COPY operations don't require any special shutdown procedures beyond what's handled in the destroy callback
- Part of PostgreSQL's destination receiver framework that separates shutdown (end of normal processing) from destroy (cleanup of resources)
- Actual resource cleanup for COPY operations occurs in copy_dest_destroy, not in this shutdown callback