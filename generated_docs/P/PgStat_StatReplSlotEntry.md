# PgStat_StatReplSlotEntry

## Location
src/include/pgstat.h: 367 - 378

## Overview
PgStat_StatReplSlotEntry is a structure that tracks statistics for logical replication slots in PostgreSQL, monitoring data spilling, streaming operations, and overall transaction processing metrics.

## Definition


## Detailed Description
PgStat_StatReplSlotEntry maintains comprehensive statistics for logical replication slots, which are essential components of PostgreSQL's logical replication system. This structure tracks both spill operations (when transaction data is written to disk due to memory constraints) and stream operations (when data is streamed directly to consumers), along with overall transaction statistics. These metrics are crucial for monitoring replication slot performance, identifying bottlenecks, and understanding the behavior of logical decoding processes.

## Parameters / Member Variables
- : Number of transactions that were spilled to disk during logical decoding
- : Total number of spill operations performed for this replication slot
- : Total number of bytes spilled to disk during logical decoding
- : Number of transactions that were streamed directly without spilling
- : Total number of streaming operations performed for this replication slot
- : Total number of bytes streamed directly during logical decoding
- : Total number of transactions processed by this replication slot
- : Total number of bytes processed by this replication slot
- : Timestamp indicating when the statistics for this replication slot were last reset

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter (statistics counter type)
  - TimestampTz (timestamp data type)
- Called from (representative examples):
  - UpdateDecodingStats (logical decoding statistics update)
  - pgstat_report_replslot (replication slot statistics reporting)
  - pgstat_drop_replslot (cleanup when replication slot is dropped)
  - pgstat_fetch_replslot (statistics retrieval for specific slot)
  - PG_STAT_GET_REPLICATION_SLOT_COLS (SQL interface macro)
  - pgstat_count_buffer_hit (buffer statistics integration)
  - [PgStatShared_ReplSlot](PgStatShared_ReplSlot.md) (shared memory statistics structure)

## Notes and Other Information
- This structure is the foundation for PostgreSQL's pg_stat_replication_slots system view
- Spill vs stream statistics help administrators understand memory pressure and logical decoding behavior
- High spill rates may indicate need for increased logical_decoding_work_mem setting
- Stream statistics show efficiency of direct processing without intermediate storage
- Total statistics provide overall replication slot activity metrics
- Used for monitoring logical replication performance and capacity planning
- Statistics are maintained per replication slot and persist across slot usage sessions
- Critical for diagnosing replication lag and logical decoding performance issues
- The spill/stream distinction helps optimize memory configuration for logical replication workloads