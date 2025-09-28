# CreateCopyDestReceiver

## Location
[src/backend/commands/copyto.c:1272-1286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyto.c#L1272-L1286)

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
  - [DestReceiver](../D/DestReceiver.md) (base structure type)

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

## Simplified Source

```c
// Simplified version of CreateCopyDestReceiver
DestReceiver *CreateCopyDestReceiver(void) {
    // Allocate memory for the COPY destination receiver
    DR_copy *copy_receiver = (DR_copy *) palloc(sizeof(DR_copy));

    // Set up the callback functions for tuple processing
    copy_receiver->pub.receiveSlot = copy_dest_receive;    // Process each tuple
    copy_receiver->pub.rStartup = copy_dest_startup;       // Initialize at start
    copy_receiver->pub.rShutdown = copy_dest_shutdown;     // Cleanup at end
    copy_receiver->pub.rDestroy = copy_dest_destroy;       // Final cleanup

    // Set the destination type to indicate COPY TO operation
    copy_receiver->pub.mydest = DestCopyOut;

    // Initialize state fields
    copy_receiver->cstate = NULL;     // Will be set later with COPY configuration
    copy_receiver->processed = 0;     // Counter for tuples processed

    // Return as base DestReceiver type
    return (DestReceiver *) copy_receiver;
}
```

Key simplifications made:
- Added descriptive variable name for clarity
- Added comments explaining each callback function's purpose
- Explained the purpose of each field initialization
- Clarified the two-phase initialization (create now, configure later)
- Focused on core logic: allocate memory, set callbacks, initialize state, return receiver