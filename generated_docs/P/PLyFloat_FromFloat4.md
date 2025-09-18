# PLyFloat_FromFloat4

## Location
src/pl/plpython/plpy_typeio.c: 558 - 563

## Overview
PLyFloat_FromFloat4 converts PostgreSQL single-precision floating-point values (float4) to Python float objects, providing a specialized and efficient conversion path for FLOAT4OID types.

## Definition
```c
static PyObject *PLyFloat_FromFloat4(PLyDatumToOb *arg, Datum d)
```

## Detailed Description
This function is a specialized input converter that transforms PostgreSQL single-precision floating-point values (float4) into Python float objects. It serves as an optimized conversion path specifically for FLOAT4OID types, providing direct numeric conversion without the overhead of string-based generic scalar conversion routines.

The function operates by:
1. **Datum extraction**: Uses DatumGetFloat4() to extract the float4 value from the PostgreSQL Datum
2. **Type conversion**: Promotes the float4 to double precision for Python compatibility
3. **Python object creation**: Creates a Python float object using PyFloat_FromDouble()

This approach ensures efficient conversion while maintaining numeric precision. The promotion from float4 to double is necessary because Python's float type is based on C's double precision floating-point representation.

## Parameters / Member Variables
- `arg`: PLyDatumToOb structure containing conversion context (not used in this simple converter)
- `d`: PostgreSQL Datum containing the float4 value to convert

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetFloat4: PostgreSQL macro to extract float4 value from Datum
  - PyFloat_FromDouble: Python C API function to create float objects from double values
- Referenced types:
  - PLyDatumToOb: Input conversion context structure
- Called from:
  - PLy_input_setup_func: Set as conversion function for FLOAT4OID types

## Notes and Other Information
- This is a static function, only accessible within the plpy_typeio.c compilation unit
- Part of the special-purpose input converters category for optimized type-specific conversions
- The arg parameter is present for consistency with the conversion function signature but is not used
- Promotes float4 to double precision to match Python's float type representation
- Returns a new Python float object with proper reference counting
- Maintains numeric precision during the float4 to double conversion
- Located in src/pl/plpython/plpy_typeio.c at lines 558-563