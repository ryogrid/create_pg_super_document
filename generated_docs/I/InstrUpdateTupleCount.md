# InstrUpdateTupleCount

## Location
[src/backend/executor/instrument.c:132-139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/instrument.c#L132-L139)

## Overview
InstrUpdateTupleCount provides a lightweight method to update tuple count statistics in an Instrumentation structure without performing full timing or resource usage measurements.

## Definition
void InstrUpdateTupleCount(Instrumentation *instr, double nTuples)

## Detailed Description
InstrUpdateTupleCount is a simple utility function that updates only the tuple count in an Instrumentation structure without triggering any timing measurements or resource usage calculations. This function is useful in scenarios where tuple counting is needed independently of performance measurement cycles, or when implementing custom instrumentation patterns that don't follow the standard InstrStartNode/InstrStopNode pairing. Unlike InstrStopNode, this function performs no timing operations, resource usage calculations, or state management, making it very lightweight and suitable for high-frequency calls where only tuple statistics are relevant.

## Parameters / Member Variables
- `instr`: Pointer to the Instrumentation structure to update
- `nTuples`: Number of tuples to add to the current tuple count

## Dependencies
- Functions called/Symbols referenced:
  - [Instrumentation](Instrumentation.md) (structure type)
- Called from (representative examples):
  - No references found in current codebase

## Notes and Other Information
This function provides the most basic instrumentation operation, focusing solely on tuple counting. It may be used in specialized execution paths where full instrumentation overhead is not desired but tuple counting is still needed for statistics or monitoring purposes. The function's simplicity makes it suitable for performance-critical code paths where minimal overhead is essential.

## Simplified Source

```c
void InstrUpdateTupleCount(Instrumentation *instr, double nTuples)
{
    // Add the new tuple count to the running total
    instr->tuplecount += nTuples;
}
```