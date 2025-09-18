# ExecStoreVirtualTuple

## Location
src/backend/executor/execTuples.c: 1639 - 1662

## Overview
Marks a TupleTableSlot as containing a valid virtual tuple after the caller has populated the slot's Datum and isnull arrays with column data.

## Definition
```c
TupleTableSlot *ExecStoreVirtualTuple(TupleTableSlot *slot)
```

## Detailed Description
ExecStoreVirtualTuple is the final step in the virtual tuple storage protocol. It marks a previously cleared slot as containing valid data after the caller has directly populated the slot's tts_values (Datum array) and tts_isnull (null flags array) with column values. This function performs minimal work - it simply clears the empty flag and sets the number of valid attributes to match the tuple descriptor.

The virtual tuple approach avoids data copying by allowing direct manipulation of the slot's internal arrays, making it efficient for scenarios where tuple data is generated programmatically or extracted from other sources. The three-step protocol (ExecClearTuple → populate arrays → ExecStoreVirtualTuple) ensures proper slot state management while maximizing performance.

## Parameters / Member Variables
- `slot`: The TupleTableSlot to mark as containing a valid virtual tuple (must be previously cleared and have populated data arrays)

## Dependencies
- Functions called/Symbols referenced:
  - TTS_EMPTY (macro to check if slot is empty)
  - TTS_FLAG_EMPTY (flag constant for empty slot state)
- Called from (representative examples):
  - [execute_attr_map_slot](../e/execute_attr_map_slot.md)
  - [CopyFrom](../C/CopyFrom.md)
  - [ExecForceStoreHeapTuple](ExecForceStoreHeapTuple.md)
  - [ExecForceStoreMinimalTuple](ExecForceStoreMinimalTuple.md)
  - [ExecStoreAllNullTuple](ExecStoreAllNullTuple.md)
  - [prepare_hash_slot](../p/prepare_hash_slot.md)
  - [FunctionNext](../F/FunctionNext.md)
  - [ExecComputeStoredGenerated](ExecComputeStoredGenerated.md)
  - [ValuesNext](../V/ValuesNext.md)

## Notes and Other Information
- This function is part of a three-step protocol for virtual tuple storage: ExecClearTuple → populate data arrays → ExecStoreVirtualTuple
- Virtual tuples avoid memory copying by working directly with the slot's internal Datum and isnull arrays
- The function performs minimal validation, trusting that the caller has properly populated the data arrays
- Commonly used in scenarios where tuple data is generated programmatically, such as function scans, value scans, and computed columns
- The slot must be in an empty state (via ExecClearTuple) before calling this function
- Sets tts_nvalid to the full number of attributes in the tuple descriptor, indicating all columns are valid
- More efficient than storing physical tuples when the data is already in deformed (column-wise) format
- Essential for many executor nodes that generate or transform tuple data without working with physical tuple representations