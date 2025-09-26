# pgstat_report_wal

## Location
[src/backend/utils/activity/pgstat_wal.c:48-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_wal.c#L48-L66)

## Overview
Reports WAL (Write-Ahead Log) and IO usage statistics to the shared statistics collector, designed for processes that generate WAL but don't call the general pgstat_report_stat() function.

## Definition
void pgstat_report_wal(bool force)

## Detailed Description
This function calculates how much WAL usage counters have increased since the last report and updates the shared WAL and IO statistics. It is specifically designed for background processes like walwriter that generate WAL activity but do not participate in the standard statistics reporting mechanism via pgstat_report_stat().

The function performs two main operations: flushing WAL statistics and flushing IO statistics. The force parameter controls whether the function should wait for lock acquisition or return immediately if locks are not available.

## Parameters / Member Variables
- : Boolean flag that controls lock acquisition behavior. When true, the function waits to acquire the pgstat shmem LWLock, ensuring statistics are flushed. When false, the function may skip flushing if locks cannot be acquired immediately.

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_flush_wal](pgstat_flush_wal.md)
  - [pgstat_flush_io](pgstat_flush_io.md)
  - [PgStat_WalStats](../P/PgStat_WalStats.md)
- Called from (representative examples):
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md)
  - [CheckpointerMain](../C/CheckpointerMain.md)
  - [HandleCheckpointerInterrupts](../H/HandleCheckpointerInterrupts.md)
  - [WalWriterMain](../W/WalWriterMain.md)
  - pgstat_count_buffer_hit

## Notes and Other Information
- This function follows the same locking strategy as pgstat.c, using the nowait parameter to avoid blocking when force is false
- Primarily used by background processes that generate significant WAL activity but operate outside the normal transaction processing flow
- Essential for maintaining accurate WAL usage statistics in PostgreSQL's statistics collection system
- The function coordinates both WAL and IO statistics reporting to provide a comprehensive view of write activity