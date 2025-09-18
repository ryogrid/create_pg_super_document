# PLyDatumToOb

## Location
[src/pl/plpython/plpy_typeio.h:57-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.h#L57-L86)

## Overview
PLyDatumToOb is a conversion structure used in PostgreSQL's PLpython extension to handle conversion of PostgreSQL Datum values to Python objects.

## Definition


## Detailed Description
PLyDatumToOb is a core data structure in PostgreSQL's PLpython extension that encapsulates all information needed to convert PostgreSQL Datum values into their corresponding Python object representations. It serves as a conversion context that contains both the conversion function and all necessary metadata about the PostgreSQL data type being converted. The structure supports various PostgreSQL types including scalars, arrays, tuples, and types with custom transforms through a discriminated union approach.

## Parameters / Member Variables
- : Function pointer to the appropriate conversion function that performs the actual Datum-to-Python-object conversion
- : PostgreSQL type OID identifying the source data type
- : Type modifier providing additional type information (e.g., precision, scale for numeric types)
- : Boolean indicating whether the type is passed by value or by reference
- : Length of the type in bytes (-1 for variable-length types)
- : Alignment requirement for the type ('c'=char, 's'=short, 'i'=int, 'd'=double)
- : Memory context where this conversion structure is allocated
- : Union containing type-specific conversion data (scalar, array, tuple, or transform)

## Dependencies
- Functions called/Symbols referenced:
  - PLyDatumToObFunc (function pointer type)
  - [PLyScalarToOb](PLyScalarToOb.md), PLyArrayToOb, PLyTupleToOb, PLyTransformToOb (union members)
  - Standard PostgreSQL types: Oid, MemoryContext, Datum
- Called from (representative examples):
  - [PLy_input_convert](PLy_input_convert.md)
  - [PLy_input_from_tuple](PLy_input_from_tuple.md)
  - [PLy_input_setup_func](PLy_input_setup_func.md)
  - [PLyProcedure](PLyProcedure.md) structure
  - Various conversion functions (PLyBool_FromBool, PLyFloat_FromFloat4, etc.)

## Notes and Other Information
The PLyDatumToOb structure is part of PostgreSQL's type conversion system for the PLpython procedural language. The conversion data structs should be regarded as private to plpy_typeio.c, though they are declared in the header file to allow other modules to define structs containing them. The val parameter in conversion functions must not be NULL, and the structure supports polymorphic conversion through the union of different conversion type specializations.