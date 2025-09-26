# PgStat_KindInfo

## Location
src/include/utils/pgstat_internal.h: 201 - 279

## Overview
PgStat_KindInfo is a metadata structure that defines the characteristics and behavior of different kinds of PostgreSQL statistics, providing configuration and callback functions for managing various types of statistical data.

## Definition


## Detailed Description
PgStat_KindInfo serves as a configuration and metadata structure that describes how different types of PostgreSQL statistics should be handled. Each statistics kind (like bgwriter, tables, databases, etc.) has its own PgStat_KindInfo instance that defines its specific characteristics, memory layout, and callback functions for operations like flushing, deleting, resetting, and serialization. This structure enables a unified framework for managing heterogeneous statistics types while allowing each type to have its own specialized behavior.

## Parameters / Member Variables
- : Boolean flag indicating whether a fixed number of stats objects exist for this kind (e.g., bgwriter stats) or if the number is variable (e.g., table stats)
- : Boolean flag determining if stats of this kind can be accessed from another database, which affects inclusion in stats snapshots
- : Boolean flag for variable-numbered stats that are identified on-disk using a name rather than PgStat_HashKey (primarily for replication slot stats)
- : The size of an entry in the shared stats hash table (pointed to by PgStatShared_HashEntry->body)
- : The offset of statistics data inside the shared stats entry, used during serialization/deserialization
- : The size of statistics data inside the shared stats entry, used during serialization/deserialization
- : The size of pending data for this kind, used for allocations (0 means no pending entry should exist)
- : Callback function for variable-numbered stats to flush pending statistics data
- : Optional callback function for variable-numbered stats to delete pending statistics data
- : Optional callback function for variable-numbered stats to reset the reset timestamp
- : Optional callback function for variable-numbered stats with named_on_disk to convert key/header to serialized name
- : Optional callback function for variable-numbered stats with named_on_disk to convert serialized name back to key
- : Callback function for fixed-numbered statistics to reset all entries
- : Callback function for fixed-numbered statistics to build snapshot for entry
- : Human-readable name of the statistics kind

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_EntryRef
  - PgStatShared_Common
  - PgStat_HashKey
  - NameData
  - TimestampTz
- Called from (representative examples):
  - pgstat_reset
  - pgstat_build_snapshot
  - pgstat_flush_pending_entries
  - pgstat_write_statsfile
  - pgstat_read_statsfile

## Notes and Other Information
- This structure is central to PostgreSQL's statistics system architecture, enabling polymorphic behavior across different statistics types
- The callback functions provide extensibility for different statistics kinds while maintaining a common interface
- The distinction between fixed_amount and variable statistics is crucial for determining memory management and access patterns
- Serialization offsets and lengths are separate from shared_size to exclude in-memory state like lwlocks during persistence operations
- The named_on_disk feature is specifically designed for replication slot statistics which require name-based identification