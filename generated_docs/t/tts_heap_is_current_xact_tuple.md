# tts_heap_is_current_xact_tuple

## Location
[src/backend/executor/execTuples.c:375-397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L375-L397)

## Overview
Checks whether a heap tuple stored in a tuple table slot was created by the current transaction by examining its transaction ID (xmin).

## Definition

```c
static bool
tts_heap_is_current_xact_tuple(TupleTableSlot *slot)
```
## Detailed Description
This function determines if a heap tuple within a tuple table slot belongs to the current transaction context. It extracts the xmin (minimum transaction ID) from the tuple header and compares it against the current transaction ID using PostgreSQL's transaction management system. The function is part of the tuple table slot operations specific to heap tuples and is used internally for transaction visibility checks.

The function requires the slot to be materialized (contain an actual HeapTuple) and will raise an error if called on a non-materialized slot, as transaction visibility cannot be determined without access to the tuple's header information.

## Parameters / Member Variables
- : A TupleTableSlot pointer that must contain a materialized heap tuple for transaction ID examination

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleTableSlot (cast target type)
  - TTS_EMPTY (macro for checking empty slots)
  - HeapTupleHeaderGetRawXmin (extracts xmin from tuple header)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md) (checks if xmin belongs to current transaction)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- The function is declared static, making it internal to the execTuples.c compilation unit
- Requires a materialized slot - will error if the slot doesn't contain a physical HeapTuple
- Used primarily for optimization decisions in tuple deforming operations where knowing transaction ownership can affect processing strategies
- Part of the heap-specific tuple table slot operations infrastructure