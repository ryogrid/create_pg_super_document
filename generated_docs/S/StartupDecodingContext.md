# StartupDecodingContext

## Location
src/backend/replication/logical/logical.c: 152 - 331

## Overview
StartupDecodingContext is a static helper function that performs the common initialization tasks for both CreateInitDecodingContext and CreateDecodingContext, setting up a complete logical decoding environment.

## Definition


## Detailed Description
This function performs comprehensive initialization of a logical decoding context by setting up all necessary components for logical replication. It creates a dedicated memory context, initializes the LogicalDecodingContext structure, and configures various callback mechanisms for handling different types of logical decoding operations.

Key responsibilities include:
1. Creating a dedicated memory context for logical decoding operations
2. Loading and validating the output plugin (unless in fast_forward mode)
3. Setting process status flags to indicate logical decoding activity
4. Allocating and configuring WAL reader and reorder buffer components
5. Setting up snapshot builder for transaction visibility
6. Configuring callback wrappers for standard, streaming, and two-phase operations
7. Determining streaming and two-phase capabilities based on available callbacks

The function handles both streaming logical replication and two-phase commit scenarios, enabling different callback sets based on the output plugin's capabilities.

## Parameters / Member Variables
- : List of options to pass to the output plugin
- : WAL position from which to start logical decoding
- : Transaction ID horizon for snapshot building
- : Whether a complete snapshot is required
- : Skip output plugin loading for fast-forward mode
- : Flag indicating if this is called during slot creation
- : WAL reading routine function pointer
- : Callback for preparing output writes
- : Callback for performing output writes  
- : Callback for progress updates

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate: Creates memory context for decoding
  - LoadOutputPlugin: Loads the specified output plugin
  - IsTransactionOrTransactionBlock: Checks transaction state
  - XLogReaderAllocate: Allocates WAL reader
  - ReorderBufferAllocate: Allocates transaction reorder buffer
  - AllocateSnapshotBuilder: Creates snapshot management component
  - makeStringInfo: Creates output string buffer
  - Various callback wrapper functions (begin_cb_wrapper, stream_start_cb_wrapper, etc.)

- Called from (representative examples):
  - CreateInitDecodingContext: During initial decoding context creation
  - CreateDecodingContext: During regular decoding context creation

## Notes and Other Information
- Static function shared between initialization and regular context creation paths
- Automatically detects streaming capabilities by checking for streaming callback availability
- Supports two-phase commit logical decoding when appropriate callbacks are present  
- Sets PROC_IN_LOGICAL_DECODING status flag only when outside transaction blocks
- Creates wrapper callbacks to add error context information to output plugin calls
- Memory allocation failures in WAL reader allocation result in out-of-memory errors
- The function establishes the foundation for all subsequent logical decoding operations