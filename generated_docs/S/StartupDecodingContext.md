# StartupDecodingContext

## Location
[src/backend/replication/logical/logical.c:152-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L152-L331)

## Overview
StartupDecodingContext is a static helper function that performs the common initialization tasks for both CreateInitDecodingContext and CreateDecodingContext, setting up a complete logical decoding environment.

## Definition

```c
static LogicalDecodingContext *
StartupDecodingContext(List *output_plugin_options,
					   XLogRecPtr start_lsn,
					   TransactionId xmin_horizon,
					   bool need_full_snapshot,
					   bool fast_forward,
					   bool in_create,
					   XLogReaderRoutine *xl_routine,
					   LogicalOutputPluginWriterPrepareWrite prepare_write,
					   LogicalOutputPluginWriterWrite do_write,
					   LogicalOutputPluginWriterUpdateProgress update_progress)
```
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
- `*output_plugin_options`: List of options to pass to the output plugin
- `start_lsn`: WAL position from which to start logical decoding
- `xmin_horizon`: Transaction ID horizon for snapshot building
- `need_full_snapshot`: Whether a complete snapshot is required
- `fast_forward`: Skip output plugin loading for fast-forward mode
- `in_create`: Flag indicating if this is called during slot creation
- `*xl_routine`: WAL reading routine function pointer
- `prepare_write`: Callback for preparing output writes
- `do_write`: Callback for performing output writes
- `update_progress`: Callback for progress updates
## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate: Creates memory context for decoding
  - [LoadOutputPlugin](../L/LoadOutputPlugin.md): Loads the specified output plugin
  - [IsTransactionOrTransactionBlock](../I/IsTransactionOrTransactionBlock.md): Checks transaction state
  - [XLogReaderAllocate](../X/XLogReaderAllocate.md): Allocates WAL reader
  - [ReorderBufferAllocate](../R/ReorderBufferAllocate.md): Allocates transaction reorder buffer
  - [AllocateSnapshotBuilder](../A/AllocateSnapshotBuilder.md): Creates snapshot management component
  - [makeStringInfo](../m/makeStringInfo.md): Creates output string buffer
  - Various callback wrapper functions (begin_cb_wrapper, stream_start_cb_wrapper, etc.)

- Called from (representative examples):
  - [CreateInitDecodingContext](../C/CreateInitDecodingContext.md): During initial decoding context creation
  - [CreateDecodingContext](../C/CreateDecodingContext.md): During regular decoding context creation

## Notes and Other Information
- Static function shared between initialization and regular context creation paths
- Automatically detects streaming capabilities by checking for streaming callback availability
- Supports two-phase commit logical decoding when appropriate callbacks are present  
- Sets PROC_IN_LOGICAL_DECODING status flag only when outside transaction blocks
- Creates wrapper callbacks to add error context information to output plugin calls
- Memory allocation failures in WAL reader allocation result in out-of-memory errors
- The function establishes the foundation for all subsequent logical decoding operations

## Simplified Source

```c
// Simplified version of StartupDecodingContext
static LogicalDecodingContext *
StartupDecodingContext(List *output_plugin_options,
                      XLogRecPtr start_lsn,
                      TransactionId xmin_horizon,
                      bool need_full_snapshot,
                      bool fast_forward,
                      bool in_create,
                      XLogReaderRoutine *xl_routine,
                      LogicalOutputPluginWriterPrepareWrite prepare_write,
                      LogicalOutputPluginWriterWrite do_write,
                      LogicalOutputPluginWriterUpdateProgress update_progress) {

    ReplicationSlot *slot = MyReplicationSlot;

    // Create memory context for logical decoding
    MemoryContext context = AllocSetContextCreate(CurrentMemoryContext,
                                                  "Logical decoding context",
                                                  ALLOCSET_DEFAULT_SIZES);
    MemoryContext old_context = MemoryContextSwitchTo(context);
    LogicalDecodingContext *ctx = palloc0(sizeof(LogicalDecodingContext));

    ctx->context = context;

    // Load output plugin (unless fast forward mode)
    if (!fast_forward)
        LoadOutputPlugin(&ctx->callbacks, NameStr(slot->data.plugin));

    // Set logical decoding process status
    if (!IsTransactionOrTransactionBlock()) {
        LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
        MyProc->statusFlags |= PROC_IN_LOGICAL_DECODING;
        ProcGlobal->statusFlags[MyProc->pgxactoff] = MyProc->statusFlags;
        LWLockRelease(ProcArrayLock);
    }

    // Initialize core components
    ctx->slot = slot;
    ctx->reader = XLogReaderAllocate(wal_segment_size, NULL, xl_routine, ctx);
    ctx->reorder = ReorderBufferAllocate();
    ctx->snapshot_builder = AllocateSnapshotBuilder(ctx->reorder, xmin_horizon,
                                                   start_lsn, need_full_snapshot,
                                                   in_create, slot->data.two_phase_at);

    // Set up callback wrappers for error handling
    ctx->reorder->begin = begin_cb_wrapper;
    ctx->reorder->apply_change = change_cb_wrapper;
    ctx->reorder->commit = commit_cb_wrapper;

    // Configure streaming support based on available callbacks
    ctx->streaming = (ctx->callbacks.stream_start_cb != NULL) ||
                    (ctx->callbacks.stream_commit_cb != NULL) ||
                    (ctx->callbacks.stream_change_cb != NULL);

    // Configure two-phase support based on available callbacks
    ctx->twophase = (ctx->callbacks.begin_prepare_cb != NULL) ||
                   (ctx->callbacks.prepare_cb != NULL) ||
                   (ctx->callbacks.commit_prepared_cb != NULL);

    // Set up output handling
    ctx->out = makeStringInfo();
    ctx->prepare_write = prepare_write;
    ctx->write = do_write;
    ctx->update_progress = update_progress;
    ctx->output_plugin_options = output_plugin_options;
    ctx->fast_forward = fast_forward;

    MemoryContextSwitchTo(old_context);
    return ctx;
}
```

Key simplifications made:
- Removed detailed error handling for out of memory conditions
- Consolidated callback wrapper assignments
- Simplified streaming and two-phase capability detection
- Abstracted detailed callback setup for all stream types
- Focused on core initialization flow