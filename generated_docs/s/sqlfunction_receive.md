# sqlfunction_receive

## Location
[src/backend/executor/functions.c:2097-2113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L2097-L2113)

## Overview
Receives and processes individual tuples for SQL function destination receivers, filtering out junk attributes and storing the cleaned tuples in a tuplestore.

## Definition
```c
static bool sqlfunction_receive(TupleTableSlot *slot, DestReceiver *self)
```

## Detailed Description
This function is the core tuple processing callback for SQL function destination receivers. It handles each tuple that flows through the execution pipeline by:

1. Casting the generic DestReceiver to the specific DR_sqlfunction structure to access SQL function-specific state
2. Filtering the input tuple using ExecFilterJunk to remove any junk attributes (system columns or temporary attributes that shouldn't be part of the final result)
3. Storing the filtered tuple into the function's tuplestore for later retrieval

The function always returns true, indicating successful processing of the tuple. This is part of the standard DestReceiver interface where the return value indicates whether to continue processing.

## Parameters / Member Variables
- `slot`: TupleTableSlot containing the tuple to be processed and stored
- `self`: Pointer to the DestReceiver structure (cast to DR_sqlfunction internally)

## Dependencies
- Functions called/Symbols referenced:
  - DR_sqlfunction (cast target for self parameter)
  - ExecFilterJunk (filters out junk attributes from the tuple)
  - tuplestore_puttupleslot (stores the filtered tuple)
- Called from (representative examples):
  - [CreateSQLFunctionDestReceiver](../C/CreateSQLFunctionDestReceiver.md) (sets this as receive callback)
  - Used within SQLFunctionCachePtr context

## Notes and Other Information
- This function is essential for SQL function execution as it captures and stores result tuples
- The filtering step ensures that only relevant data attributes are stored, not internal system columns
- The tuplestore accumulates all result tuples which can later be read back by the calling function
- Part of the DestReceiver callback interface specifically designed for SQL function execution
- Located in src/backend/executor/functions.c along with other SQL function execution infrastructure