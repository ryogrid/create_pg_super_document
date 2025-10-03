# InstrInit

## Location
[src/backend/executor/instrument.c:58-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/instrument.c#L58-L67)

## Overview
InstrInit initializes a pre-allocated Instrumentation structure with specified instrumentation options for PostgreSQL query execution monitoring.

## Definition
void InstrInit(Instrumentation *instr, int instrument_options)

## Detailed Description
InstrInit is a utility function that reinitializes an existing Instrumentation structure, typically used when reusing instrumentation objects or when initializing structures that were allocated differently than through InstrAlloc. The function first clears all fields using memset to ensure a clean state, then selectively enables instrumentation features based on the provided options. This approach is more lightweight than allocating new structures and is particularly useful in parallel execution contexts where instrumentation structures need to be reinitialized for worker processes.

## Parameters / Member Variables
- `instr`: Pointer to a pre-allocated Instrumentation structure to be initialized
- `instrument_options`: Bitfield specifying which instrumentation features to enable (INSTRUMENT_TIMER, INSTRUMENT_BUFFERS, INSTRUMENT_WAL)

## Dependencies
- Functions called/Symbols referenced:
  - memset (memory clearing)
  - [Instrumentation](Instrumentation.md) (structure type)
  - INSTRUMENT_BUFFERS (constant)
  - INSTRUMENT_WAL (constant)
  - INSTRUMENT_TIMER (constant)
- Called from (representative examples):
  - [ExecInitParallelPlan](../E/ExecInitParallelPlan.md)

## Notes and Other Information
Unlike InstrAlloc, this function operates on pre-allocated memory and doesn't handle async_mode configuration. It's commonly used in parallel execution scenarios where instrumentation structures need to be reinitialized for worker processes. The function ensures consistent behavior by completely zeroing the structure before setting the required flags.

## Simplified Source

```c
void InstrInit(Instrumentation *instr, int instrument_options)
{
    // Clear all fields to ensure clean state
    memset(instr, 0, sizeof(Instrumentation));

    // Enable specific instrumentation features based on options
    instr->need_bufusage = (instrument_options & INSTRUMENT_BUFFERS) != 0;
    instr->need_walusage = (instrument_options & INSTRUMENT_WAL) != 0;
    instr->need_timer = (instrument_options & INSTRUMENT_TIMER) != 0;
}
```