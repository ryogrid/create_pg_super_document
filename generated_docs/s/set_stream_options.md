# set_stream_options

## Location
src/backend/replication/logical/worker.c: 4351 - 4400

## Overview
A function that configures streaming options for logical replication workers, setting up protocol parameters based on server version and subscription settings.

## Definition
```c
void set_stream_options(WalRcvStreamOptions *options, char *slotname, XLogRecPtr *origin_startpos)
```

## Detailed Description
set_stream_options is responsible for initializing and configuring the streaming options structure used by logical replication workers. The function sets up various parameters required for establishing a logical replication connection, including protocol version negotiation based on the server version, streaming mode configuration, and publication settings.

The function performs server version detection to determine the appropriate logical replication protocol version to use, enabling features like streaming, two-phase commit, and parallel apply based on server capabilities. It configures streaming modes (off, on, or parallel) depending on both the subscription configuration and server support.

Key configuration steps include:
1. Setting basic logical replication parameters (slot name, start position)
2. Determining protocol version based on server version
3. Configuring streaming mode and parallel apply settings
4. Setting publication names and binary transfer options
5. Configuring origin and two-phase commit settings

## Parameters / Member Variables
- `options`: Pointer to WalRcvStreamOptions structure to be configured with streaming parameters
- `slotname`: Name of the replication slot to use for this connection
- `origin_startpos`: Pointer to XLogRecPtr indicating the starting position for replication

## Dependencies
- Functions called/Symbols referenced:
  - walrcv_server_version (retrieves server version information)
  - WalRcvStreamOptions (structure type for streaming options)
  - LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM (protocol version constant)
  - LOGICALREP_PROTO_TWOPHASE_VERSION_NUM (protocol version constant)
  - LOGICALREP_PROTO_STREAM_VERSION_NUM (protocol version constant)
  - LOGICALREP_PROTO_VERSION_NUM (base protocol version constant)
  - LOGICALREP_STREAM_PARALLEL (streaming mode constant)
  - LOGICALREP_STREAM_OFF (streaming disabled constant)
- Called from (representative examples):
  - run_tablesync_worker (at src/backend/replication/logical/tablesync.c:1727)
  - run_apply_worker (at src/backend/replication/logical/worker.c:4534)

## Notes and Other Information
- Server version thresholds: 16.0+ supports parallel streaming, 15.0+ supports two-phase commit, 14.0+ supports basic streaming
- Sets parallel_apply flag in MyLogicalRepWorker based on streaming configuration and server capabilities
- Always sets twophase to false in the current implementation
- Uses subscription settings (MySubscription) to determine binary transfer mode, publications, and origin
- Essential for proper logical replication worker initialization and protocol negotiation