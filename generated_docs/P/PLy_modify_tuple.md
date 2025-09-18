# PLy_modify_tuple

## Location
[src/pl/plpython/plpy_exec.c:922-1051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L922-L1051)

## Overview
Applies row modifications requested by a PL/Python trigger function by converting Python dictionary changes back to PostgreSQL tuple format and creating a modified HeapTuple.

## Definition


## Detailed Description
This function processes modifications to a row tuple as specified by a PL/Python trigger function. It extracts the 'new' dictionary from the trigger data (pltd), validates its structure, and applies the changes to create a new HeapTuple. The function iterates through all keys in the 'new' dictionary, validates that they correspond to valid, modifiable table columns, converts Python values to PostgreSQL Datums using the appropriate conversion functions, and constructs arrays for values, nulls, and replacement flags. It prevents modification of system attributes, generated columns, and validates column existence. The function uses PostgreSQL's heap_modify_tuple to create the final modified tuple.

## Parameters / Member Variables
- : PL/Python procedure containing result conversion information and attribute details
- : Python trigger data dictionary containing the 'new' key with modified values
- : Trigger context data including relation information and tuple descriptors
- : Original HeapTuple to be modified

## Dependencies
- Functions called/Symbols referenced:
  - [plpython_trigger_error_callback](../p/plpython_trigger_error_callback.md)
  - [PLyUnicode_AsString](PLyUnicode_AsString.md)
  - [SPI_fnumber](../S/SPI_fnumber.md)
  - [PLy_output_convert](PLy_output_convert.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - TupleDescAttr
  - RelationGetDescr
  - PyDict_GetItemString
  - PyDict_Keys
  - PyList_Size
- Called from (representative examples):
  - [PLy_exec_trigger](PLy_exec_trigger.md)

## Notes and Other Information
The function performs extensive validation including:
- Ensuring the 'new' key exists in the trigger data dictionary
- Verifying 'new' is a dictionary object
- Checking that all dictionary keys are strings representing valid column names
- Preventing modification of system attributes (attn <= 0)
- Blocking changes to generated columns
- Validating column existence in the table schema

The function uses error context callbacks for better error reporting and PG_TRY/PG_CATCH blocks for proper resource cleanup. Memory is allocated for modvalues, modnulls, and modrepls arrays which track the new values, null status, and which columns should be replaced. These are automatically freed in both success and error paths.