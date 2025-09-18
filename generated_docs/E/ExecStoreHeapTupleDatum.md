# ExecStoreHeapTupleDatum

## Location
src/backend/executor/execTuples.c: 1693 - 1730

## Overview
Stores a HeapTuple in datum form into a TupleTableSlot by deforming it and storing it in virtual form, allowing access to the tuple's individual column values.

## Definition
```c
void ExecStoreHeapTupleDatum(Datum data, TupleTableSlot *slot)
```

## Detailed Description
This function takes a HeapTuple that is stored in datum form (compressed/serialized format) and stores it into a TupleTableSlot in virtual form. The key operation is deforming the tuple, which means extracting individual column values from the binary tuple format and storing them as separate Datum values in the slot's arrays. This allows for efficient access to individual columns without having to repeatedly parse the binary tuple format.

The function creates a temporary HeapTupleData structure to hold the tuple metadata, then uses heap_deform_tuple to extract the individual column values into the slot's tts_values and tts_isnull arrays. Until the slot is materialized, the slot contents depend on the original datum remaining valid.

## Parameters / Member Variables
- `data`: A Datum containing a HeapTuple in serialized form
- `slot`: The TupleTableSlot where the deformed tuple data will be stored

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetHeapTupleHeader
  - HeapTupleHeaderGetDatumLength  
  - ExecClearTuple
  - [heap_deform_tuple](../h/heap_deform_tuple.md)
  - [ExecStoreVirtualTuple](ExecStoreVirtualTuple.md)
- Called from (representative examples):
  - [EvalPlanQualFetchRowMark](EvalPlanQualFetchRowMark.md)
  - TupIsNull

## Notes and Other Information
- The function always stores the tuple in virtual form rather than minimal or heap form
- The slot contents remain dependent on the input datum until the slot is materialized
- This is part of PostgreSQL's tuple slot abstraction that provides uniform access to tuples regardless of their storage format
- The function clears any existing tuple data in the slot before storing the new tuple