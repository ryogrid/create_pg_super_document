# PLyTransformToOb

## Location
[src/pl/plpython/plpy_typeio.h:52-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.h#L52-L55)

## Overview
PLyTransformToOb is a struct that contains conversion information for transforming PostgreSQL data types using custom transform functions to Python objects.

## Definition


## Detailed Description
PLyTransformToOb handles conversion of PostgreSQL data types that have custom transform functions defined for the PL/Python language. Transform functions provide a way to customize how specific PostgreSQL types are converted to Python objects, bypassing the standard conversion mechanisms. This allows for more efficient or specialized representations of complex data types.

## Parameters / Member Variables
- : FmgrInfo struct containing cached lookup information for the from-SQL transform function that converts PostgreSQL values to Python objects

## Dependencies
- Functions called/Symbols referenced:
  - [FmgrInfo](../F/FmgrInfo.md) (PostgreSQL function manager structure)
- Called from (representative examples):
  - [PLyDatumToOb](PLyDatumToOb.md) (as part of the union)
  - [PLyObject_FromTransform](PLyObject_FromTransform.md)

## Notes and Other Information
Transform functions are registered in PostgreSQL using CREATE TRANSFORM and provide a mechanism for type authors to define custom conversion behavior. This struct is used when such transforms are available and preferred over the default conversion methods for better performance or functionality.