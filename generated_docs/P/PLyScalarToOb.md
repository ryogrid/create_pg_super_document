# PLyScalarToOb

## Location
[src/pl/plpython/plpy_typeio.h:30-33](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.h#L30-L33)

## Overview
PLyScalarToOb is a struct that contains conversion information for transforming PostgreSQL scalar types to Python objects.

## Definition


## Detailed Description
PLyScalarToOb is a simple struct used within the PLyDatumToOb union to handle conversion of scalar PostgreSQL data types to Python objects. It stores the function manager information needed to call the PostgreSQL type's output function, which converts the internal representation to a string that can then be processed into a Python object.

## Parameters / Member Variables
- : FmgrInfo struct containing cached lookup information for the PostgreSQL type's output function

## Dependencies
- Functions called/Symbols referenced:
  - [FmgrInfo](../F/FmgrInfo.md) (PostgreSQL function manager structure)
- Called from (representative examples):
  - [PLyDatumToOb](PLyDatumToOb.md) (as part of the union)

## Notes and Other Information
This struct is part of the type-specific conversion data stored in PLyDatumToOb's union. It represents the simplest case of data conversion where PostgreSQL's built-in type output functions are used to convert values to string representation before creating Python objects.