# PLyBool_FromBool

## Location
[src/pl/plpython/plpy_typeio.c:550-557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L550-L557)

## Overview
PLyBool_FromBool converts PostgreSQL boolean values to Python boolean objects, providing a specialized and optimized conversion path for BOOLOID types.

## Definition
```c
static PyObject *PLyBool_FromBool(PLyDatumToOb *arg, Datum d)
```

## Detailed Description
This function is a specialized input converter that transforms PostgreSQL boolean values into Python boolean objects. It serves as an optimized conversion path specifically for BOOLOID types, avoiding the overhead of generic scalar conversion routines.

The function operates by:
1. **Datum extraction**: Uses DatumGetBool() to extract the boolean value from the PostgreSQL Datum
2. **Python boolean creation**: Returns the appropriate Python boolean singleton (Py_True or Py_False)
3. **Reference management**: Uses Python's Py_RETURN_TRUE and Py_RETURN_FALSE macros which properly handle reference counting

This specialized converter is more efficient than generic scalar conversion since it directly maps PostgreSQL boolean values to Python boolean objects without string conversion or other intermediate steps.

## Parameters / Member Variables
- `arg`: PLyDatumToOb structure containing conversion context (not used in this simple converter)
- `d`: PostgreSQL Datum containing the boolean value to convert

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetBool](../D/DatumGetBool.md): PostgreSQL macro to extract boolean value from Datum
  - Py_RETURN_TRUE: Python macro returning True singleton with proper reference counting
  - Py_RETURN_FALSE: Python macro returning False singleton with proper reference counting
- Referenced types:
  - [PLyDatumToOb](PLyDatumToOb.md): Input conversion context structure
- Called from:
  - [PLy_input_setup_func](PLy_input_setup_func.md): Set as conversion function for BOOLOID types

## Notes and Other Information
- This is a static function, only accessible within the plpy_typeio.c compilation unit
- Part of the special-purpose input converters category for optimized type-specific conversions
- The arg parameter is present for consistency with the conversion function signature but is not used
- Uses PostgreSQL's DatumGetBool macro for efficient boolean extraction
- Returns Python boolean singletons, ensuring proper memory management
- Located in src/pl/plpython/plpy_typeio.c at lines 550-557

## Simplified Source

```c
static PyObject *PLyBool_FromBool(PLyDatumToOb *arg, Datum d)
{
    // Convert PostgreSQL boolean to Python boolean singleton
    if (DatumGetBool(d))
        Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}
```