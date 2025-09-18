# WalRcvStreamOptions

## Location
src/include/replication/walreceiver.h: 191 - 193

## Overview
WalRcvStreamOptions is a configuration structure that specifies options for starting a WAL receiver stream, supporting both physical and logical replication with protocol-specific parameters.

## Definition
```c
typedef struct
{
    bool        logical;        /* True if this is logical replication stream,
                                 * false if physical stream.  */
    char       *slotname;       /* Name of the replication slot or NULL. */
    XLogRecPtr  startpoint;     /* LSN of starting point. */

    union
    {
        struct
        {
            TimeLineID  startpointTLI;  /* Starting timeline */
        }           physical;
        struct
        {
            uint32      proto_version;  /* Logical protocol version */
            List       *publication_names;  /* String list of publications */
            bool        binary; /* Ask publisher to use binary */
            char       *streaming_str;  /* Streaming of large transactions */
            bool        twophase;   /* Streaming of two-phase transactions at
                                     * prepare time */
            char       *origin; /* Only publish data originating from the
                                 * specified origin */
        }           logical;
    }           proto;
} WalRcvStreamOptions;
```

## Detailed Description
WalRcvStreamOptions serves as a comprehensive configuration structure for establishing WAL receiver streams in PostgreSQL replication. It uses a discriminated union design to handle different replication types (physical vs. logical) efficiently, providing type-specific options while maintaining a common interface.

For physical replication, the structure focuses on timeline management and LSN positioning. For logical replication, it provides extensive configuration options including protocol version negotiation, publication filtering, binary format preferences, large transaction streaming, two-phase commit handling, and origin filtering.

The structure is typically populated by higher-level replication management functions and passed to lower-level streaming functions to configure the replication connection appropriately.

## Parameters / Member Variables
- `logical`: Boolean flag determining replication type (true for logical, false for physical)
- `slotname`: Name of the replication slot to use, or NULL if no slot is specified
- `startpoint`: LSN (Log Sequence Number) indicating where streaming should begin
- `proto.physical.startpointTLI`: Timeline ID for the starting point (physical replication only)
- `proto.logical.proto_version`: Protocol version number for logical replication
- `proto.logical.publication_names`: List of publication names to subscribe to (logical replication)
- `proto.logical.binary`: Flag requesting binary format from publisher instead of text
- `proto.logical.streaming_str`: Configuration string for streaming large transactions
- `proto.logical.twophase`: Enable streaming of two-phase transactions at prepare time
- `proto.logical.origin`: Filter to only receive data originating from specified origin

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtr
  - TimeLineID
  - [List](../L/List.md) (PostgreSQL list structure)
  - uint32

- Called from (representative examples):
  - [libpqrcv_startstreaming](../l/libpqrcv_startstreaming.md)
  - [run_tablesync_worker](../r/run_tablesync_worker.md)
  - [set_stream_options](../s/set_stream_options.md)
  - [run_apply_worker](../r/run_apply_worker.md)
  - [WalReceiverMain](WalReceiverMain.md)

## Notes and Other Information
- Uses a discriminated union to efficiently handle different replication protocols
- The `logical` field serves as the discriminator for the union, determining which struct members are valid
- Physical replication options are minimal, focusing primarily on timeline management
- Logical replication options are extensive, reflecting the complexity of logical replication protocols
- The structure is designed to be forward-compatible with future protocol extensions
- Binary format option in logical replication can significantly improve performance for large datasets
- Two-phase transaction support enables distributed transaction scenarios
- Origin filtering allows selective replication in multi-master or cascading replication setups
- Used as a parameter for establishing both libpq-based and other types of replication connections