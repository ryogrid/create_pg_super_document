# PgStat_CheckpointerStats

## Location
src/include/pgstat.h: 261 - 272

## Overview
PgStat_CheckpointerStats tracks comprehensive performance and activity statistics for PostgreSQL's checkpointer process, including checkpoint and restartpoint operations, timing metrics, and buffer I/O statistics.

## Definition


## Detailed Description
PgStat_CheckpointerStats maintains detailed statistics for PostgreSQL's checkpointer process, which is responsible for writing dirty buffers to disk at regular intervals and during shutdown. The checkpointer ensures data durability by creating consistent recovery points and manages WAL recycling. In standby servers, the checkpointer creates restartpoints instead of checkpoints. This structure tracks both the frequency and performance characteristics of these critical operations, including timing information that helps administrators understand I/O performance and system load patterns.

## Parameters / Member Variables
- : Counter tracking the number of scheduled checkpoints triggered by timeout (checkpoint_timeout parameter)
- : Counter tracking the number of checkpoints requested by other processes (e.g., due to WAL segment threshold)
- : Counter tracking the number of scheduled restartpoints on standby servers
- : Counter tracking the number of requested restartpoints on standby servers  
- : Counter tracking the total number of restartpoints actually completed on standby servers
- : Counter tracking the total time spent writing buffers to disk during checkpoints/restartpoints (in milliseconds)
- : Counter tracking the total time spent syncing (fsync) files during checkpoints/restartpoints (in milliseconds)
- : Counter tracking the total number of buffers written by the checkpointer process
- : Timestamp indicating when these checkpointer statistics were last reset to zero

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter
  - TimestampTz
- Called from (representative examples):
  - pgstat_report_checkpointer
  - pgstat_checkpointer_snapshot_cb
  - pgstat_count_buffer_hit
  - PgStatShared_Checkpointer
  - PgStat_Snapshot

## Notes and Other Information
Checkpointer statistics are crucial for database performance tuning and understanding I/O patterns. High write_time and sync_time values may indicate storage performance issues or the need for checkpoint tuning. The ratio of timed vs requested checkpoints can reveal whether the system is checkpoint-bound or has appropriate checkpoint intervals configured. For standby servers, restartpoint statistics help monitor replication lag and recovery performance. These statistics are accessible through PostgreSQL's pg_stat_bgwriter view despite the naming, as the checkpointer functionality was split from the background writer in later PostgreSQL versions.