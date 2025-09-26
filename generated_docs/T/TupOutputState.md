# TupOutputState

## Location
[src/include/executor/executor.h:505-509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/executor.h#L505-L509)

## Overview
TupOutputState is a simple state structure used to manage tuple output operations in PostgreSQL, encapsulating a tuple slot and destination receiver for sending result tuples to the frontend or other specified destinations.

## Definition

```c
typedef struct TupOutputState
{
	TupleTableSlot *slot;
	DestReceiver *dest;
} TupOutputState;
```
## Detailed Description
TupOutputState serves as a lightweight wrapper that combines a TupleTableSlot and a DestReceiver to facilitate tuple output operations. This structure is primarily used by utility commands (such as EXPLAIN and SHOW ALL) that need to project results directly to a destination without requiring full table function capabilities.

The structure provides a simplified interface for:
- Creating temporary tuple slots for output formatting
- Managing the destination receiver for result delivery
- Coordinating the lifecycle of tuple output operations

This design pattern allows utility commands to leverage the existing tuple transmission infrastructure while maintaining a clean separation of concerns between tuple storage (slot) and tuple delivery (destination receiver).

## Parameters / Member Variables
- : A TupleTableSlot pointer that holds the current tuple being processed for output. This slot is used as a temporary container for formatting and transmitting individual tuples.
- : A DestReceiver pointer that defines where the output tuples should be sent (e.g., to the frontend client, to a file, or to another processing component).

## Dependencies
- Functions called/Symbols referenced:
  - [TupleTableSlot](TupleTableSlot.md) (referenced as member type)
  - [DestReceiver](../D/DestReceiver.md) (referenced as member type)
- Called from (representative examples):
  - [begin_tup_output_tupdesc](../b/begin_tup_output_tupdesc.md) (initializes TupOutputState)
  - [do_tup_output](../d/do_tup_output.md) (uses TupOutputState to send tuples)
  - [end_tup_output](../e/end_tup_output.md) (cleans up TupOutputState)
  - [ExplainQuery](../E/ExplainQuery.md) (in EXPLAIN command implementation)
  - [ShowGUCConfigOption](../S/ShowGUCConfigOption.md) (in SHOW command implementation)
  - [ExecuteCallStmt](../E/ExecuteCallStmt.md) (in function call execution)

## Notes and Other Information
- [TupOutputState](TupOutputState.md) is designed for utility commands that need simple tuple output functionality without the overhead of full executor node infrastructure.
- The structure follows PostgreSQL's pattern of combining related state into compact structures for efficient memory management.
- Memory management is handled by the caller - the TupOutputState itself is allocated and freed by the begin_tup_output_tupdesc/end_tup_output function pair.
- The slot member uses MakeSingleTupleTableSlot() for creation, optimized for single-tuple operations.
- This structure is part of the executor subsystem but serves as a bridge between the executor's tuple handling mechanisms and the frontend communication protocol.
- Location: src/include/executor/executor.h:505-509