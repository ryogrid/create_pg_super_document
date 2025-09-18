# CreateCopyDestReceiver

## Location
src/backend/commands/copyto.c: 1272 - 1286

## Overview
CreateCopyDestReceiver creates and initializes a DestReceiver object specifically designed for handling COPY TO operations in PostgreSQL, setting up the necessary function pointers and initial state for processing query results and sending them through the COPY protocol.

## Definition
```c
DestReceiver *CreateCopyDestReceiver(void)
```

## Detailed Description
CreateCopyDestReceiver is a factory function that creates a specialized DestReceiver for COPY TO operations. It allocates and initializes a DR_copy structure, which extends the base DestReceiver with COPY-specific functionality. The function sets up all required callback functions that the PostgreSQL executor will use to process query results and format them according to the COPY protocol.

The function initializes the DestReceiver with four key callback functions:
- receiveSlot: Processes each tuple received from the executor
- rStartup: Handles initialization when execution begins
- rShutdown: Handles cleanup when execution ends  
- rDestroy: Handles final cleanup and memory deallocation

The created receiver is marked with DestCopyOut destination type, indicating it handles COPY TO operations. The cstate field is initially NULL and will be set later when the COPY operation is configured.

## Parameters / Member Variables
This function takes no parameters. The returned DR_copy structure contains:
- `pub`: Base DestReceiver structure with callback function pointers
- `cstate`: CopyToState containing COPY operation configuration (initially NULL)
- `processed`: Counter for number of tuples processed (initialized to 0)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [copy_dest_receive](../c/copy_dest_receive.md) (tuple processing callback)
  - [copy_dest_startup](../c/copy_dest_startup.md) (startup callback)
  - [copy_dest_shutdown](../c/copy_dest_shutdown.md) (shutdown callback)  
  - [copy_dest_destroy](../c/copy_dest_destroy.md) (cleanup callback)
  - DestCopyOut (destination type constant)
  - DR_copy (structure type)
  - DestReceiver (base structure type)

- Called from (representative examples):
  - [CreateDestReceiver](CreateDestReceiver.md) (in dest.c:145 - general destination receiver factory)
  - Referenced in CopyToState (in copy.h:114 - COPY operation state)

## Notes and Other Information
- This function is part of PostgreSQL's COPY TO implementation, which exports query results in various formats
- The DR_copy structure extends the base DestReceiver pattern, allowing the executor to treat COPY operations uniformly with other result destinations
- The actual COPY operation configuration (cstate) must be set separately after calling this function
- The function uses PostgreSQL's memory management (palloc) for allocation
- Progress reporting is handled in the copy_dest_receive callback, updating the PROGRESS_COPY_TUPLES_PROCESSED parameter
- Located in src/backend/commands/copyto.c:1272-1286