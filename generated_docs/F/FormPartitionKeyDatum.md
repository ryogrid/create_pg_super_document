# FormPartitionKeyDatum

## Location
[src/backend/executor/execPartition.c:1294-1347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L1294-L1347)

## Overview
Constructs values[] and isnull[] arrays for the partition key of a tuple by extracting and evaluating partition key attributes and expressions.

## Definition
```c
static void FormPartitionKeyDatum(PartitionDispatch pd, TupleTableSlot *slot, EState *estate, Datum *values, bool *isnull)
```

## Detailed Description
This function extracts partition key values from a tuple to support partition pruning and routing decisions. It handles two types of partition key components:

1. **Plain columns**: Directly extracts attribute values from the tuple using slot_getattr()
2. **Expressions**: Evaluates partition key expressions using the executor's expression evaluation framework

The function performs lazy initialization of expression evaluation state on first use and ensures proper context setup for expression evaluation. It validates that the number of partition key expressions matches expectations and populates the output arrays with the extracted/computed partition key values and their null indicators.

## Parameters / Member Variables
- `pd`: PartitionDispatch object containing partition key information and expression state
- `slot`: TupleTableSlot containing the heap tuple from which to extract partition key values
- `estate`: EState (executor state) required for evaluating partition key expressions (must be non-NULL)
- `values`: Output array to store the computed partition key Datum values
- `isnull`: Output array to store null indicators corresponding to each partition key value

## Dependencies
- Functions called/Symbols referenced:
  - GetPerTupleExprContext
  - [ExecPrepareExprList](../E/ExecPrepareExprList.md)
  - [list_head](../l/list_head.md)
  - [slot_getattr](../s/slot_getattr.md)
  - [ExecEvalExprSwitchContext](../E/ExecEvalExprSwitchContext.md)
  - [lnext](../l/lnext.md)
- Called from (representative examples):
  - [ExecFindPartition](../E/ExecFindPartition.md) (for determining the correct partition for tuple routing)

## Notes and Other Information
- The function requires that estate's per-tuple expression context (ecxt_scantuple) points to the input slot
- Expression evaluation state (pd->keystate) is lazily initialized on first use for performance
- The function distinguishes between plain column references (keycol != 0) and expressions (keycol == 0)
- Expression evaluation uses ExecEvalExprSwitchContext to ensure proper memory context management
- Error checking ensures the number of partition key expressions matches the expected count
- This is a static function used internally within the partition routing subsystem