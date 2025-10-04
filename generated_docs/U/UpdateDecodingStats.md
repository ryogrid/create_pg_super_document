# UpdateDecodingStats

## Location
[src/backend/replication/logical/logical.c:1979-2025](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1979-L2025)

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
  - [ReorderBuffer](../R/ReorderBuffer.md) (from ctx->reorder)
  - [PgStat_StatReplSlotEntry](../P/PgStat_StatReplSlotEntry.md) (statistics structure)
  - [pgstat_report_replslot](../p/pgstat_report_replslot.md) (reports to statistics collector)
  - elog (DEBUG2 logging)
- Called from (representative examples):
  - [DecodeCommit](../D/DecodeCommit.md)
  - [DecodePrepare](../D/DecodePrepare.md)  
  - [DecodeAbort](../D/DecodeAbort.md)
  - [ReorderBufferSerializeTXN](../R/ReorderBufferSerializeTXN.md)
  - [ReorderBufferStreamTXN](../R/ReorderBufferStreamTXN.md)

## Notes and Other Information
- Implements delta reporting by resetting counters after each report
- Includes detailed debug logging showing all collected metrics
- Early exit optimization prevents unnecessary work when no activity occurred
- Part of PostgreSQL's comprehensive monitoring and observability infrastructure
- Statistics reported include both transaction counts and byte volumes for different operation types

## Simplified Source

```c
void
UpdateDecodingStats(LogicalDecodingContext *ctx)
{
    ReorderBuffer *rb = ctx->reorder;
    PgStat_StatReplSlotEntry repSlotStat;

    // Early exit if no activity to report
    if (rb->spillBytes <= 0 && rb->streamBytes <= 0 && rb->totalBytes <= 0)
        return;

    // Debug logging of current statistics
    elog(DEBUG2, "UpdateDecodingStats: updating stats %p %lld %lld %lld %lld %lld %lld %lld %lld",
         rb,
         (long long) rb->spillTxns, (long long) rb->spillCount, (long long) rb->spillBytes,
         (long long) rb->streamTxns, (long long) rb->streamCount, (long long) rb->streamBytes,
         (long long) rb->totalTxns, (long long) rb->totalBytes);

    // Copy statistics from reorder buffer to report structure
    repSlotStat.spill_txns = rb->spillTxns;
    repSlotStat.spill_count = rb->spillCount;
    repSlotStat.spill_bytes = rb->spillBytes;
    repSlotStat.stream_txns = rb->streamTxns;
    repSlotStat.stream_count = rb->streamCount;
    repSlotStat.stream_bytes = rb->streamBytes;
    repSlotStat.total_txns = rb->totalTxns;
    repSlotStat.total_bytes = rb->totalBytes;

    // Report statistics to PostgreSQL stats collector
    pgstat_report_replslot(ctx->slot, &repSlotStat);

    // Reset counters for delta reporting
    rb->spillTxns = 0;
    rb->spillCount = 0;
    rb->spillBytes = 0;
    rb->streamTxns = 0;
    rb->streamCount = 0;
    rb->streamBytes = 0;
    rb->totalTxns = 0;
    rb->totalBytes = 0;
}
```