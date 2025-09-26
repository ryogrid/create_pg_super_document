# Instrumentation

## Location
src/include/executor/instrument.h: 68 - 93

## Overview
Instrumentation is the core struct that tracks detailed execution statistics for query plan nodes, including timing, tuple counts, buffer usage, and WAL usage for performance analysis and EXPLAIN output.

## Definition
```c
typedef struct Instrumentation
{
    /* Parameters set at node creation: */
    bool        need_timer;         /* true if we need timer data */
    bool        need_bufusage;      /* true if we need buffer usage data */
    bool        need_walusage;      /* true if we need WAL usage data */
    bool        async_mode;         /* true if node is in async mode */
    /* Info about current plan cycle: */
    bool        running;            /* true if we've completed first tuple */
    instr_time  starttime;          /* start time of current iteration of node */
    instr_time  counter;            /* accumulated runtime for this node */
    double      firsttuple;         /* time for first tuple of this cycle */
    double      tuplecount;         /* # of tuples emitted so far this cycle */
    BufferUsage bufusage_start;     /* buffer usage at start */
    WalUsage    walusage_start;     /* WAL usage at start */
    /* Accumulated statistics across all completed cycles: */
    double      startup;            /* total startup time (in seconds) */
    double      total;              /* total time (in seconds) */
    double      ntuples;            /* total tuples produced */
    double      ntuples2;           /* secondary node-specific tuple counter */
    double      nloops;             /* # of run cycles for this node */
    double      nfiltered1;         /* # of tuples removed by scanqual or joinqual */
    double      nfiltered2;         /* # of tuples removed by "other" quals */
    BufferUsage bufusage;           /* total buffer usage */
    WalUsage    walusage;           /* total WAL usage */
} Instrumentation;
```

## Detailed Description
Instrumentation is the central data structure for PostgreSQL's query execution profiling and monitoring system. It provides comprehensive tracking of execution metrics for individual plan nodes, enabling detailed performance analysis through EXPLAIN ANALYZE and similar tools.

The struct is organized into three logical sections: configuration parameters that determine what data to collect, current execution state for the active cycle, and accumulated statistics across all execution cycles. This design supports both real-time monitoring during execution and comprehensive reporting after completion.

The instrumentation system supports both synchronous and asynchronous execution modes and integrates closely with the buffer management and WAL systems to provide complete I/O impact analysis.

## Parameters / Member Variables
### Configuration Parameters:
- `need_timer`: Flag indicating whether timing information should be collected
- `need_bufusage`: Flag indicating whether buffer usage statistics should be tracked
- `need_walusage`: Flag indicating whether WAL usage statistics should be tracked
- `async_mode`: Flag indicating if the node operates in asynchronous mode

### Current Execution State:
- `running`: Flag indicating if the node has produced its first tuple
- `starttime`: Start time of the current execution iteration
- `counter`: Accumulated runtime for the current node
- `firsttuple`: Time taken to produce the first tuple in the current cycle
- `tuplecount`: Number of tuples emitted in the current cycle
- `bufusage_start`: Buffer usage counters at the start of current execution
- `walusage_start`: WAL usage counters at the start of current execution

### Accumulated Statistics:
- `startup`: Total startup time across all cycles (in seconds)
- `total`: Total execution time across all cycles (in seconds)
- `ntuples`: Total number of tuples produced
- `ntuples2`: Secondary tuple counter for node-specific metrics
- `nloops`: Number of execution cycles (loops) for this node
- `nfiltered1`: Number of tuples filtered out by scan or join qualifiers
- `nfiltered2`: Number of tuples filtered out by other types of qualifiers
- `bufusage`: Accumulated buffer usage statistics
- `walusage`: Accumulated WAL usage statistics

## Dependencies
- Functions called/Symbols referenced:
  - instr_time (timing infrastructure)
  - BufferUsage (buffer I/O tracking)
  - WalUsage (WAL activity tracking)
- Called from (representative examples):
  - InstrAlloc
  - InstrInit
  - InstrStartNode
  - InstrStopNode
  - InstrUpdateTupleCount
  - InstrEndLoop
  - InstrAggNode
  - ExplainNode (EXPLAIN output)
  - PlanState (executor nodes)

## Notes and Other Information
- The struct is defined in src/include/executor/instrument.h:68-93
- Core component of PostgreSQL's EXPLAIN ANALYZE functionality
- Supports both timing and resource usage tracking with configurable granularity
- Used extensively in parallel query execution for aggregating worker statistics
- Essential for query optimization and performance tuning
- Integrates with trigger execution monitoring and reporting
- The ntuples2 field provides flexibility for node-specific tuple counting schemes
- Supports multiple execution cycles (loops) which can occur in nested loop joins and similar operations