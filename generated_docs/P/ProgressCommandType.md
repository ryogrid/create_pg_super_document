# ProgressCommandType

## Location
[src/include/utils/backend_progress.h:31-32](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/backend_progress.h#L31-L32)

## Overview
An enumeration that defines the types of long-running database commands that can report progress information to PostgreSQL's progress monitoring system.

## Definition


## Detailed Description
The ProgressCommandType enum is part of PostgreSQL's progress reporting infrastructure, which allows administrators to monitor the progress of long-running database operations. This enum categorizes different types of commands that can provide real-time progress information through the PostgreSQL statistics system. Each command type corresponds to a specific view in pg_stat_progress_* system views that administrators can query to monitor operation status.

The progress reporting system uses a standardized interface where each command type defines its own set of progress parameters (up to PGSTAT_NUM_PROGRESS_PARAM=20 parameters) that get updated during execution. The meaning of these parameters varies by command type and is defined in commands/progress.h and exposed through system views.

## Parameters / Member Variables
- : Invalid or uninitialized command type, used as a sentinel value
- : VACUUM operations (lazy vacuum), tracks heap blocks scanned, vacuumed, index processing, etc.
- : ANALYZE operations for gathering table statistics, tracks sample acquisition and statistics computation
- : CLUSTER and VACUUM FULL operations that reorganize table storage, tracks tuple scanning and rewriting
- : Index creation operations including CREATE INDEX, REINDEX, and concurrent variants, tracks tuple processing and build phases
- : Base backup operations (pg_basebackup), tracks backup streaming and tablespace processing
- : COPY operations for data import/export, tracks bytes and tuples processed

## Dependencies
- Functions called/Symbols referenced:
  - None (this is an enum definition)
- Called from (representative examples):
  - [pgstat_progress_start_command](../p/pgstat_progress_start_command.md)
  - PG_STAT_GET_PROGRESS_COLS
  - [PgBackendStatus](PgBackendStatus.md) (as a field type)

## Notes and Other Information
- Defined in src/include/utils/backend_progress.h:22-31
- Each command type has associated progress parameter definitions in commands/progress.h
- Progress information is exposed through pg_stat_progress_* system views
- The progress reporting system supports up to 20 parameters per command (PGSTAT_NUM_PROGRESS_PARAM)
- Extensions can potentially add new command types, though the enum would need to be extended
- Progress reporting is designed for operations that may take significant time and benefit from user visibility into their progress