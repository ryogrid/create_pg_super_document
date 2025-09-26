# pltcl_build_tuple_argument

## Location
src/pl/tcl/pltcl.c: 3104 - 3179

## Overview
Builds a Tcl list object suitable for 'array set' from all attributes of a given tuple, converting PostgreSQL tuple data into a format that can be consumed by Tcl functions.

## Definition

```c
static Tcl_Obj *
pltcl_build_tuple_argument(HeapTuple tuple, TupleDesc tupdesc, bool include_generated)
```
## Detailed Description
This function converts a PostgreSQL HeapTuple into a Tcl list object that can be used with Tcl's 'array set' command. It iterates through all attributes in the tuple descriptor, extracts each attribute's name and value from the tuple, and creates a Tcl list containing alternating attribute names and their string representations. The function handles type conversion by using PostgreSQL's output functions to convert each attribute value to its string representation, then converts the strings from PostgreSQL's encoding to UTF-8 for Tcl compatibility.

The function skips dropped attributes and can optionally include or exclude generated columns based on the include_generated parameter. For non-null values, it retrieves the appropriate output function for the attribute's data type and converts the value to a string format that Tcl can understand.

## Parameters / Member Variables
- : The HeapTuple containing the row data to be converted
- : The TupleDesc describing the structure and metadata of the tuple
- : Boolean flag indicating whether to include generated columns in the result

## Dependencies
- Functions called/Symbols referenced:
  - heap_getattr
  - getTypeOutputInfo  
  - OidOutputFunctionCall
  - UTF_BEGIN
  - UTF_E2U
  - UTF_END
  - Tcl_NewObj
  - Tcl_ListObjAppendElement
  - Tcl_NewStringObj
  - TupleDescAttr
  - NameStr
  - pfree
- Called from (representative examples):
  - pltcl_func_handler
  - pltcl_trigger_handler

## Notes and Other Information
- The function creates a flat list suitable for Tcl's 'array set' command, where each pair of consecutive elements represents an attribute name and its value
- The function handles UTF-8 encoding conversion using UTF_BEGIN/UTF_E2U/UTF_END macros to ensure proper character encoding between PostgreSQL and Tcl
- Null attributes are skipped entirely rather than being represented in the list, which may require careful handling by calling functions
- Generated columns are only included when explicitly requested via the include_generated parameter
- Memory management is handled by freeing the outputstr after use with pfree()