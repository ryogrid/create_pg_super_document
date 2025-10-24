# processXactStats

## Location
[src/bin/pgbench/pgbench.c:4681-4719](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L4681-L4719)

## Overview
Accumulates and reports statistics at the end of a transaction, handling both successful and failed/skipped transactions with latency and lag calculations.

## Definition
```c
static void processXactStats(TState *thread, CState *st, pg_time_usec_t *now, bool skipped, StatsData *agg)
```

## Detailed Description
This function is responsible for collecting and processing transaction statistics in pgbench after each transaction completion. It calculates latency and lag metrics for successful transactions, accumulates statistics at both thread and per-script levels, tracks transactions that exceed latency limits, and triggers logging when enabled. The function handles various reporting modes including progress reporting, throttle delay tracking, latency limiting, detailed logging, and per-script statistics. It's designed to be called for all transaction outcomes, including skipped and failed transactions.

## Parameters / Member Variables
- `thread`: Pointer to TState structure containing thread-specific data and statistics
- `st`: Pointer to CState structure representing the client state with transaction details
- `now`: Pointer to pg_time_usec_t timestamp, used lazily for performance optimization
- `skipped`: Boolean flag indicating whether the transaction was skipped
- `agg`: Pointer to StatsData structure for aggregated statistics (used for logging)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_time_now_lazy](pg_time_now_lazy.md) (lazy timestamp function)
  - [accumStats](../a/accumStats.md) (statistics accumulation - called twice)
  - [doLog](../d/doLog.md) (transaction logging)
  - Types: TState, CState, StatsData, pg_time_usec_t
  - Constants: ESTATUS_NO_ERROR
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md) (at src/bin/pgbench/pgbench.c:3734, 4210)

## Notes and Other Information
- This is a static function, only accessible within pgbench.c
- Called for all transaction outcomes, including skipped and failed transactions
- Uses lazy timestamp evaluation (pg_time_now_lazy) for performance optimization
- Calculates latency as the time from transaction scheduling to completion
- Calculates lag as the delay between intended start time and actual start time
- Increments counters for transactions exceeding the latency limit when configured
- Updates multiple statistics levels: thread-level, client-level, and optionally per-script level
- Contains a note about potential mutex usage for per-script stats but chooses not to use one
- The function comment notes that even skipped and failed transactions are counted in the client's transaction count

## Simplified Source

```c
static void processXactStats(TState *thread, CState *st, pg_time_usec_t *now,
                            bool skipped, StatsData *agg) {
    double latency = 0.0, lag = 0.0;
    bool detailed = progress || throttle_delay || latency_limit ||
                   use_log || per_script_stats;

    // Calculate latency and lag for successful, non-skipped transactions
    if (detailed && !skipped && st->estatus == ESTATUS_NO_ERROR) {
        pg_time_now_lazy(now);

        // Latency: time from scheduled start to completion
        latency = (*now) - st->txn_scheduled;

        // Lag: delay between intended and actual start time
        lag = st->txn_begin - st->txn_scheduled;
    }

    // Accumulate statistics at thread level
    accumStats(&thread->stats, skipped, latency, lag, st->estatus, st->tries);

    // Track transactions exceeding latency limit
    if (latency_limit && latency > latency_limit)
        thread->latency_late++;

    // Increment client transaction counter
    st->cnt++;

    // Trigger logging if enabled
    if (use_log)
        doLog(thread, st, agg, skipped, latency, lag);

    // Accumulate per-script statistics if enabled
    if (per_script_stats)
        accumStats(&sql_script[st->use_file].stats, skipped, latency, lag,
                  st->estatus, st->tries);
}
```