# InstrAlloc

## Location
[src/backend/executor/instrument.c:31-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/instrument.c#L31-L57)

## Overview
InstrAlloc allocates and initializes an array of Instrumentation structures for PostgreSQL query execution performance monitoring and profiling.

## Definition
Instrumentation *InstrAlloc(int n, int instrument_options, bool async_mode)

## Detailed Description
InstrAlloc is a core function in PostgreSQL's execution instrumentation system that allocates memory for performance monitoring structures. The function creates an array of Instrumentation structures and configures them based on the requested instrumentation options. It supports three main types of instrumentation: timing measurements (INSTRUMENT_TIMER), buffer usage tracking (INSTRUMENT_BUFFERS), and Write-Ahead Log usage tracking (INSTRUMENT_WAL). The function uses palloc0 to ensure all fields are zero-initialized for consistent behavior, then selectively enables instrumentation features based on the provided options.

## Parameters / Member Variables
- `n`: Number of Instrumentation structures to allocate in the array
- `instrument_options`: Bitfield specifying which instrumentation features to enable (INSTRUMENT_TIMER, INSTRUMENT_BUFFERS, INSTRUMENT_WAL)
- `async_mode`: Boolean flag indicating whether instrumentation should operate in asynchronous mode

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (memory allocation)
  - [Instrumentation](Instrumentation.md) (structure type)
  - INSTRUMENT_TIMER (constant)
  - INSTRUMENT_BUFFERS (constant)
  - INSTRUMENT_WAL (constant)
- Called from (representative examples):
  - [InitResultRelInfo](InitResultRelInfo.md)
  - [ExecInitNode](../E/ExecInitNode.md)

## Notes and Other Information
The function follows PostgreSQL's memory management conventions by using palloc0 for zero-initialized allocation. The instrumentation options are processed using bitwise operations to enable specific monitoring features. All allocated Instrumentation structures in the array share the same configuration based on the provided parameters, ensuring consistent monitoring behavior across multiple execution nodes.

## Simplified Source

```c
Instrumentation *
InstrAlloc(int n, int instrument_options, bool async_mode)
{
    Instrumentation *instr;

    // Allocate array of instrumentation structures, zero-initialized
    instr = palloc0(n * sizeof(Instrumentation));

    // Configure instrumentation features if any are requested
    if (instrument_options & (INSTRUMENT_BUFFERS | INSTRUMENT_TIMER | INSTRUMENT_WAL)) {
        bool need_buffers = (instrument_options & INSTRUMENT_BUFFERS) != 0;
        bool need_wal = (instrument_options & INSTRUMENT_WAL) != 0;
        bool need_timer = (instrument_options & INSTRUMENT_TIMER) != 0;

        // Apply configuration to all structures in the array
        for (int i = 0; i < n; i++) {
            instr[i].need_bufusage = need_buffers;
            instr[i].need_walusage = need_wal;
            instr[i].need_timer = need_timer;
            instr[i].async_mode = async_mode;
        }
    }

    return instr;
}
```