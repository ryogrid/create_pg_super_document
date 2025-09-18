# LogicalDecodingContext

## Location
src/include/replication/logical.h: 33 - 115

## Overview
LogicalDecodingContext is a central control structure that manages all aspects of logical replication decoding in PostgreSQL. It maintains the state, configuration, and infrastructure needed to decode WAL records into logical changes for replication plugins.

## Definition
```c
typedef struct LogicalDecodingContext
{
    /* memory context this is all allocated in */
    MemoryContext context;

    /* The associated replication slot */
    ReplicationSlot *slot;

    /* infrastructure pieces for decoding */
    XLogReaderState *reader;
    struct ReorderBuffer *reorder;
    struct SnapBuild *snapshot_builder;

    /*
     * Marks the logical decoding context as fast forward decoding one. Such a
     * context does not have plugin loaded so most of the following properties
     * are unused.
     */
    bool        fast_forward;

    OutputPluginCallbacks callbacks;
    OutputPluginOptions options;

    /*
     * User specified options
     */
    List       *output_plugin_options;

    /*
     * User-Provided callback for writing/streaming out data.
     */
    LogicalOutputPluginWriterPrepareWrite prepare_write;
    LogicalOutputPluginWriterWrite write;
    LogicalOutputPluginWriterUpdateProgress update_progress;

    /*
     * Output buffer.
     */
    StringInfo    out;

    /*
     * Private data pointer of the output plugin.
     */
    void       *output_plugin_private;

    /*
     * Private data pointer for the data writer.
     */
    void       *output_writer_private;

    /*
     * Does the output plugin support streaming, and is it enabled?
     */
    bool        streaming;

    /*
     * Does the output plugin support two-phase decoding, and is it enabled?
     */
    bool        twophase;

    /*
     * Is two-phase option given by output plugin?
     *
     * This flag indicates that the plugin passed in the two-phase option as
     * part of the START_STREAMING command. We cant rely solely on the
     * twophase flag which only tells whether the plugin provided all the
     * necessary two-phase callbacks.
     */
    bool        twophase_opt_given;

    /*
     * State for writing output.
     */
    bool        accept_writes;
    bool        prepared_write;
    XLogRecPtr    write_location;
    TransactionId write_xid;
    /* Are we processing the end LSN of a transaction? */
    bool        end_xact;

    /* Do we need to process any change in fast_forward mode? */
    bool        processing_required;
} LogicalDecodingContext;
```

## Detailed Description
The LogicalDecodingContext structure serves as the central control hub for PostgreSQL logical replication decoding. It encapsulates all necessary components and state required to transform WAL (Write-Ahead Log) records into meaningful logical changes that can be consumed by replication plugins.

The context manages the entire decoding pipeline: from reading WAL records through the XLogReaderState, reordering transactions via ReorderBuffer, building consistent snapshots with SnapBuild, to finally outputting decoded changes through plugin callbacks. It supports both streaming and two-phase commit decoding modes, and includes a fast-forward mode for efficient slot advancement without full decoding.

The structure maintains plugin-specific state, output buffers, and writer callbacks, enabling flexible output handling for different replication scenarios. It also tracks transaction boundaries and write states to ensure consistent and ordered delivery of logical changes.

## Parameters / Member Variables
- `context`: Memory context for all allocations related to this decoding session
- `slot`: Associated replication slot that tracks the decoding position and state
- `reader`: XLogReaderState for reading and parsing WAL records
- `reorder`: ReorderBuffer for collecting and reordering transaction changes
- `snapshot_builder`: SnapBuild for constructing consistent snapshot states
- `fast_forward`: Flag indicating fast-forward mode without plugin loading
- `callbacks`: Output plugin callback functions for processing decoded changes
- `options`: Configuration options for the output plugin
- `output_plugin_options`: User-specified options passed to the output plugin
- `prepare_write`: Callback function to prepare for writing output data
- `write`: Callback function to write output data
- `update_progress`: Callback function to update decoding progress
- `out`: String buffer for accumulating output data
- `output_plugin_private`: Private data pointer for output plugin use
- `output_writer_private`: Private data pointer for output writer use
- `streaming`: Flag indicating if streaming decoding is enabled
- `twophase`: Flag indicating if two-phase commit decoding is enabled
- `twophase_opt_given`: Flag indicating if two-phase option was explicitly provided
- `accept_writes`: Flag controlling whether writes are currently accepted
- `prepared_write`: Flag indicating if a write operation is prepared
- `write_location`: Current WAL location being written
- `write_xid`: Transaction ID currently being written
- `end_xact`: Flag indicating if processing transaction end LSN
- `processing_required`: Flag indicating if processing is needed in fast-forward mode

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlot](../R/ReplicationSlot.md)
  - [ReorderBuffer](../R/ReorderBuffer.md)
  - [SnapBuild](../S/SnapBuild.md)
  - [OutputPluginCallbacks](../O/OutputPluginCallbacks.md)
  - [OutputPluginOptions](../O/OutputPluginOptions.md)
  - LogicalOutputPluginWriterPrepareWrite
- Called from (representative examples):
  - [LogicalDecodingProcessRecord](LogicalDecodingProcessRecord.md)
  - StartupDecodingContext
  - CreateDecodingContext
  - CreateInitDecodingContext
  - [FreeDecodingContext](../F/FreeDecodingContext.md)
  - [pgoutput_startup](../p/pgoutput_startup.md)
  - [WalSndPrepareWrite](../W/WalSndPrepareWrite.md)

## Notes and Other Information
- The structure is defined in src/include/replication/logical.h:33-115
- Essential for PostgreSQL logical replication and logical decoding functionality
- Used extensively throughout the logical decoding subsystem including decode.c, logical.c, and plugin implementations
- The fast_forward mode allows efficient slot advancement without full plugin processing
- Two-phase commit support enables proper handling of prepared transactions in logical replication
- Streaming support allows real-time processing of large transactions without waiting for commit
- Memory management is centralized through the context field for cleanup efficiency