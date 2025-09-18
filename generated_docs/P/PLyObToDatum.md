# PLyObToDatum

## Location
[src/pl/plpython/plpy_typeio.h:87-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.h#L87-L92)

## Overview
PLyObToDatum is a conversion structure used in PostgreSQL's PLpython extension to handle conversion of Python objects to PostgreSQL Datum values.

## Definition
```c
typedef struct PLyObToDatum PLyObToDatum;  /* forward reference */

typedef Datum (*PLyObToDatumFunc) (PLyObToDatum *arg, PyObject *val,
                                   bool *isnull,
                                   bool inarray);

struct PLyObToDatum
{
    PLyObToDatumFunc func;      /* conversion control function */
    Oid              typoid;    /* OID of the target type */
    int32            typmod;    /* typmod of the target type */
    bool             typbyval;  /* its physical representation details */
    int16            typlen;
    char             typalign;
    MemoryContext    mcxt;      /* context this info is stored in */
    union                       /* conversion-type-specific data */
    {
        PLyObToScalar scalar;
        PLyObToArray array;
        PLyObToTuple tuple;
        PLyObToDomain domain;
        PLyObToTransform transform;
    } u;
};
```

## Detailed Description
PLyObToDatum is the counterpart to PLyDatumToOb, handling the reverse conversion from Python objects back to PostgreSQL Datum values. This structure encapsulates all necessary information and metadata required for converting Python values into their corresponding PostgreSQL data representations. The conversion function must be called even for Python None values to ensure domain constraints can be properly checked. The structure supports various PostgreSQL target types including scalars, arrays, tuples, domains, and types with custom transforms.

## Parameters / Member Variables
- `func`: Function pointer to the appropriate conversion function that performs the Python-object-to-Datum conversion
- `typoid`: PostgreSQL type OID identifying the target data type
- `typmod`: Type modifier providing additional target type information
- `typbyval`: Boolean indicating whether the target type is passed by value or by reference  
- `typlen`: Length of the target type in bytes (-1 for variable-length types)
- `typalign`: Alignment requirement for the target type
- `mcxt`: Memory context where this conversion structure is allocated
- `u`: Union containing type-specific conversion data (scalar, array, tuple, domain, or transform)

## Dependencies
- Functions called/Symbols referenced:
  - PLyObToDatumFunc (function pointer type)  
  - [PLyObToScalar](PLyObToScalar.md), PLyObToArray, PLyObToTuple, PLyObToDomain, PLyObToTransform (union members)
  - Standard PostgreSQL types: Oid, MemoryContext, Datum
  - Python C API types: PyObject
- Called from (representative examples):
  - [PLy_output_convert](PLy_output_convert.md)
  - [PLy_output_setup_func](PLy_output_setup_func.md)
  - [PLyProcedure](PLyProcedure.md) structure
  - Various conversion functions (PLyObject_ToBool, PLyObject_ToScalar, etc.)
  - [PLy_spi_prepare](PLy_spi_prepare.md), PLy_spi_execute_plan

## Notes and Other Information
The PLyObToDatum structure is essential for PostgreSQL's PLpython procedural language output conversion system. The isnull parameter in the conversion function is set to true if the Python value is None, and false otherwise. The inarray parameter indicates if the converted value was within a Python list/array context, which helps provide better error messages. Domain constraint checking requires that the conversion function be called even for None values. The structure follows the same memory management principles as PLyDatumToOb, with conversion data being private to plpy_typeio.c.