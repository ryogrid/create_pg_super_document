# tstoreReceiveSlot_notoast

## Location
[src/backend/executor/tstoreReceiver.c:122-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/tstoreReceiver.c#L122-L135)

## Overview
A lightweight callback function that receives tuples from the executor and stores them directly in a tuplestore without any detoasting or tuple format conversion processing.

## Definition
```c
static bool tstoreReceiveSlot_notoast(TupleTableSlot *slot, DestReceiver *self)
```

## Detailed Description
This function represents the simplest and most efficient path for storing tuples in a tuplestore destination receiver. It is used when the incoming tuples do not require detoasting of variable-length attributes and no tuple format conversion is needed. The function directly passes the tuple slot to `tuplestore_puttupleslot` for storage, making it the fastest of the three tuplestore receiver callback variants.

This callback is selected by `tstoreStartupReceiver` when analysis determines that neither detoasting nor tuple mapping operations are required, providing optimal performance for straightforward tuple storage scenarios.

## Parameters / Member Variables
- `slot`: TupleTableSlot containing the tuple to be stored
- `self`: Pointer to the DestReceiver structure (cast to TStoreState internally)

## Dependencies
- Functions called/Symbols referenced:
  - [tuplestore_puttupleslot](tuplestore_puttupleslot.md)
- Called from (representative examples):
  - Set as callback by tstoreStartupReceiver
  - Referenced in TStoreState structure
  - Used by CreateTuplestoreDestReceiver

## Notes and Other Information
- This is a static function used as a callback within the tuplestore receiver framework
- Represents the optimal performance path when no tuple processing is required
- Always returns true to indicate successful tuple reception
- Part of a three-function callback system (notoast, detoast, tupmap) chosen based on tuple characteristics
- The function name explicitly indicates it does not perform detoasting operations