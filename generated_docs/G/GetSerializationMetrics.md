# GetSerializationMetrics

## Location
[src/backend/commands/explain.c:5581-5592](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L5581-L5592)

## Overview
Retrieves performance metrics from a destination receiver, returning actual serialization metrics if it's a SerializeDestReceiver or zero-initialized metrics otherwise.

## Definition
```c
static SerializeMetrics GetSerializationMetrics(DestReceiver *dest)
```

## Detailed Description
This function safely extracts serialization performance metrics from a destination receiver. It performs type checking to ensure the receiver is actually a SerializeDestReceiver (by checking if mydest equals DestExplainSerialize) before accessing its metrics. If the receiver is not a SerializeDestReceiver (such as in cases where the statement is CREATE TABLE AS with an IntoRel receiver), the function returns a zero-initialized SerializeMetrics structure. This defensive approach prevents accessing invalid memory and provides consistent behavior across different receiver types.

## Parameters / Member Variables
- `dest`: Pointer to the DestReceiver from which to extract metrics

## Dependencies
- Functions called/Symbols referenced:
  - memset
  - INSTR_TIME_SET_ZERO
  - [SerializeMetrics](../S/SerializeMetrics.md)
  - DestExplainSerialize
  - [SerializeDestReceiver](../S/SerializeDestReceiver.md)
- Called from (representative examples):
  - [ExplainOnePlan](../E/ExplainOnePlan.md)
  - BYTES_TO_KILOBYTES

## Notes and Other Information
- The function implements type-safe metric extraction by checking the receiver type before casting
- Returns zero-initialized metrics for non-SerializeDestReceiver types to maintain consistent API behavior
- Handles the special case of CREATE TABLE AS statements that use IntoRel receivers instead of SerializeDestReceivers
- The function is defensive in nature, preventing crashes from invalid receiver type assumptions
- Part of the performance monitoring infrastructure for EXPLAIN (SERIALIZE) operations
- The returned SerializeMetrics structure contains timing and other performance data collected during serialization