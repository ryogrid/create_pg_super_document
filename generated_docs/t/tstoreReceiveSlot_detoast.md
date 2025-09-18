# tstoreReceiveSlot_detoast

## Location
src/backend/executor/tstoreReceiver.c: 136 - 191

## Overview
A specialized callback function that receives tuples from the executor, detoasts any externally-stored variable-length attributes, and stores the processed tuples in a tuplestore.

## Definition
```c
static bool tstoreReceiveSlot_detoast(TupleTableSlot *slot, DestReceiver *self)
```

## Detailed Description
This function handles the complex case where incoming tuples contain variable-length attributes that may be stored externally (toasted). It systematically processes each tuple by examining all attributes and detoasting any that are stored out-of-line. The function creates a new array of datums with all toasted values expanded in-place, ensuring that the tuplestore contains fully materialized tuple data.

The detoasting process involves:
1. Fully deconstructing the input tuple slot
2. Scanning all non-dropped variable-length attributes 
3. Identifying externally stored values using VARATT_IS_EXTERNAL
4. Calling detoast_external_attr to retrieve the actual data
5. Building a new datum array with detoasted values
6. Storing the processed tuple using tuplestore_putvalues
7. Cleaning up any temporary detoasted values

## Parameters / Member Variables
- `slot`: TupleTableSlot containing the tuple with potentially toasted attributes
- `self`: Pointer to the DestReceiver structure (cast to TStoreState internally)

## Dependencies
- Functions called/Symbols referenced:
  - slot_getallattrs
  - TupleDescAttr
  - VARATT_IS_EXTERNAL
  - DatumGetPointer
  - detoast_external_attr
  - PointerGetDatum
  - MemoryContextSwitchTo
  - tuplestore_putvalues
  - pfree
- Called from (representative examples):
  - Set as callback by tstoreStartupReceiver when detoasting is required
  - Referenced in TStoreState structure

## Notes and Other Information
- This function is selected when tstoreStartupReceiver detects variable-length attributes (attlen == -1)
- Uses workspace arrays (outvalues, tofree) allocated during startup for efficient processing
- Maintains proper memory context management to ensure detoasted values are cleaned up
- Only processes non-dropped attributes to avoid unnecessary work
- The tofree array tracks temporary detoasted values that need cleanup after tuple storage
- More expensive than tstoreReceiveSlot_notoast but ensures complete tuple materialization
- Cannot be used simultaneously with tuple conversion (Assert in startup ensures mutual exclusion)