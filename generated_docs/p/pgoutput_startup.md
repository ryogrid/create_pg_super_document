# pgoutput_startup

## Location
[src/backend/replication/pgoutput/pgoutput.c:434-573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L434-L573)

## Overview
The initialization function for the pgoutput logical replication plugin that sets up memory contexts, validates parameters, and configures plugin state.

## Definition
```c
static void pgoutput_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
```

## Detailed Description
This function performs comprehensive initialization of the pgoutput plugin during logical replication startup. It creates dedicated memory contexts for the plugin's operations, validates protocol version compatibility, processes configuration parameters, and sets up caching mechanisms. The function handles two scenarios: actual replication startup (when is_init is false) and slot initialization (when is_init is true). During replication startup, it performs extensive parameter validation, protocol version checking, and feature compatibility verification for streaming and two-phase commit functionality. It also registers system cache callbacks for publication changes and initializes the relation schema cache.

## Parameters / Member Variables
- `ctx`: Logical decoding context containing replication state and configuration
- `opt`: Output plugin options structure to be configured
- `is_init`: Boolean indicating whether this is slot initialization (true) or replication startup (false)

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (logical decoding context structure)
  - [OutputPluginOptions](../O/OutputPluginOptions.md) (output plugin options structure)
  - [PGOutputData](../P/PGOutputData.md) (plugin private data structure)
  - [MemoryContextCallback](../M/MemoryContextCallback.md) (memory context callback structure)
  - AllocSetContextCreate (memory context creation function)
  - ALLOCSET_DEFAULT_SIZES (default memory context sizes)
  - ALLOCSET_SMALL_SIZES (small memory context sizes)
  - [pgoutput_pubctx_reset_callback](pgoutput_pubctx_reset_callback.md) (publication context reset callback)
  - [MemoryContextRegisterResetCallback](../M/MemoryContextRegisterResetCallback.md) (register memory context callback)
  - OUTPUT_PLUGIN_BINARY_OUTPUT (binary output type constant)
  - [parse_output_parameters](parse_output_parameters.md) (parameter parsing function)
  - LOGICALREP_PROTO_MAX_VERSION_NUM (maximum protocol version)
  - LOGICALREP_PROTO_MIN_VERSION_NUM (minimum protocol version)
  - LOGICALREP_STREAM_OFF (streaming disabled constant)
  - LOGICALREP_STREAM_ON (streaming enabled constant)
  - LOGICALREP_STREAM_PARALLEL (parallel streaming constant)
  - LOGICALREP_PROTO_STREAM_VERSION_NUM (streaming protocol version)
  - LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM (parallel streaming protocol version)
  - LOGICALREP_PROTO_TWOPHASE_VERSION_NUM (two-phase commit protocol version)
  - [CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md) (system cache callback registration)
  - [publication_invalidation_cb](publication_invalidation_cb.md) (publication cache invalidation callback)
  - [init_rel_sync_cache](../i/init_rel_sync_cache.md) (relation synchronization cache initialization)
- Called from:
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (plugin initialization callback registration)

## Notes and Other Information
- Creates three separate memory contexts: main context, cache context, and publication context
- Validates protocol version compatibility and reports detailed error messages for version mismatches
- Handles streaming mode configuration with support for regular and parallel streaming
- Manages two-phase commit feature enablement based on protocol version and context capabilities
- Registers publication invalidation callbacks to handle schema changes
- Disables advanced features (streaming, two-phase) during slot initialization mode
- Sets output type to binary format for efficient data transmission
- Implements comprehensive error checking with appropriate PostgreSQL error codes
- Uses static variables to track callback registration across multiple plugin instances

## Simplified Source

```c
static void pgoutput_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init) {
    PGOutputData *data = palloc0(sizeof(PGOutputData));
    static bool publication_callback_registered = false;

    // Create memory contexts
    data->context = AllocSetContextCreate(ctx->context, "logical replication output context",
                                          ALLOCSET_DEFAULT_SIZES);
    data->cachectx = AllocSetContextCreate(ctx->context, "logical replication cache context",
                                           ALLOCSET_DEFAULT_SIZES);
    pubctx = AllocSetContextCreate(ctx->context, "logical replication publication list context",
                                   ALLOCSET_SMALL_SIZES);

    // Register memory context callback
    MemoryContextCallback *mcallback = palloc0(sizeof(MemoryContextCallback));
    mcallback->func = pgoutput_pubctx_reset_callback;
    MemoryContextRegisterResetCallback(ctx->context, mcallback);

    ctx->output_plugin_private = data;
    opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

    if (!is_init) {
        // Parse and validate parameters
        parse_output_parameters(ctx->output_plugin_options, data);

        // Validate protocol version
        if (data->protocol_version > LOGICALREP_PROTO_MAX_VERSION_NUM ||
            data->protocol_version < LOGICALREP_PROTO_MIN_VERSION_NUM)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("unsupported protocol version")));

        // Configure streaming and two-phase based on protocol version
        if (data->streaming != LOGICALREP_STREAM_OFF) {
            if (data->protocol_version < LOGICALREP_PROTO_STREAM_VERSION_NUM ||
                !ctx->streaming)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("streaming not supported")));
        }

        if (data->two_phase) {
            if (data->protocol_version < LOGICALREP_PROTO_TWOPHASE_VERSION_NUM ||
                !ctx->twophase)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("two-phase commit not supported")));
            ctx->twophase_opt_given = true;
        }

        // Initialize publication state and register callbacks
        data->publications = NIL;
        publications_valid = false;

        if (!publication_callback_registered) {
            CacheRegisterSyscacheCallback(PUBLICATIONOID, publication_invalidation_cb, (Datum) 0);
            publication_callback_registered = true;
        }

        init_rel_sync_cache(CacheMemoryContext);
    } else {
        // Disable advanced features during slot initialization
        ctx->streaming = false;
        ctx->twophase = false;
    }
}
```