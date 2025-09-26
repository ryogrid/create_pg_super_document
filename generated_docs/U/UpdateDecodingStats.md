# UpdateDecodingStats

## Location
src/backend/replication/logical/logical.c: 1979 - 2025

## Overview
Reports statistics for a logical replication slot by collecting accumulated metrics from the reorder buffer and sending them to the PostgreSQL statistics collector.

## Definition
void UpdateDecodingStats(LogicalDecodingContext *ctx)

## Detailed Description
This function is responsible for collecting and reporting performance metrics related to logical replication decoding operations. It extracts statistics from the reorder buffer within the logical decoding context and reports them to PostgreSQL's statistics subsystem.

The function tracks several key metrics:
- Spill operations: transactions and data that exceeded memory limits and were written to disk
- Stream operations: transactions that were streamed in chunks due to size
- Total operations: overall transaction and byte counts processed

After collecting the statistics, the function resets all counters in the reorder buffer to zero, ensuring that subsequent calls only report new activity. This implements a delta reporting mechanism where each call reports activity since the last report.

The function includes an early exit optimization - if no meaningful activity has occurred (all byte counters are zero or negative), it returns immediately without performing any reporting.

## Parameters / Member Variables
- : LogicalDecodingContext containing the reorder buffer with accumulated statistics

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBuffer (from ctx->reorder)
  - PgStat_StatReplSlotEntry (statistics structure)
  - pgstat_report_replslot (reports to statistics collector)
  - elog (DEBUG2 logging)
- Called from (representative examples):
  - DecodeCommit
  - DecodePrepare  
  - DecodeAbort
  - ReorderBufferSerializeTXN
  - ReorderBufferStreamTXN

## Notes and Other Information
- Implements delta reporting by resetting counters after each report
- Includes detailed debug logging showing all collected metrics
- Early exit optimization prevents unnecessary work when no activity occurred
- Part of PostgreSQL's comprehensive monitoring and observability infrastructure
- Statistics reported include both transaction counts and byte volumes for different operation types