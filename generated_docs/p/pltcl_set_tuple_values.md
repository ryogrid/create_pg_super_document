# pltcl_set_tuple_values

## Location
src/pl/tcl/pltcl.c: 3018 - 3103

## Overview
pltcl_set_tuple_values is a static function in the PL/Tcl extension that sets Tcl variables for all attributes of a given PostgreSQL tuple, making tuple data accessible to Tcl code.

## Definition
```c
static void
pltcl_set_tuple_values(Tcl_Interp *interp, const char *arrayname,
                       uint64 tupno, HeapTuple tuple, TupleDesc tupdesc)
```

## Detailed Description
pltcl_set_tuple_values extracts all column values from a PostgreSQL tuple and creates corresponding Tcl variables to make the data accessible to Tcl code. The function can operate in two modes: setting individual variables (when arrayname is NULL) or populating a Tcl array (when arrayname is provided). For each non-dropped attribute in the tuple, it retrieves the value, converts it to a string representation using the appropriate output function, and sets the corresponding Tcl variable. NULL values cause the corresponding Tcl variable to be unset. When using array mode, it also sets a special ".tupno" element with the current tuple number. The function handles proper UTF-8 encoding conversion between PostgreSQL and Tcl.

## Parameters / Member Variables
- `interp`: Tcl interpreter context where variables will be created
- `arrayname`: Name of Tcl array to populate (NULL for individual variables)
- `tupno`: Tuple number to store in the ".tupno" array element
- `tuple`: PostgreSQL HeapTuple containing the data to extract
- `tupdesc`: Tuple descriptor defining the structure and types of the tuple

## Dependencies
- Functions called/Symbols referenced:
  - heap_getattr
  - getTypeOutputInfo
  - OidOutputFunctionCall
  - TupleDescAttr
  - UTF_E2U (encoding conversion)
  - pstrdup
  - pfree
  - unconstify
  - Tcl_SetVar2Ex
  - Tcl_UnsetVar2
  - Tcl_NewStringObj
  - Tcl_NewWideIntObj
- Called from (representative examples):
  - pltcl_process_SPI_result (multiple times)

## Notes and Other Information
- Assumes arrayname parameter is in UTF-8 encoding (typically from Tcl)
- Skips dropped attributes automatically
- Handles NULL values by unsetting the corresponding Tcl variable
- Uses PostgreSQL's type-specific output functions for data conversion
- Manages memory properly with pfree() calls
- Supports both individual variable mode and array mode operations
- Sets ".tupno" element when in array mode to track tuple numbers
- Part of the result processing mechanism in PL/Tcl
- Enables seamless access to PostgreSQL tuple data from Tcl code