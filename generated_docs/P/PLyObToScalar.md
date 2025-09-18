# PLyObToScalar

## Location
[src/pl/plpython/plpy_typeio.h:93-97](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.h#L93-L97)

## Overview
PLyObToScalar is a specialized conversion structure used within PostgreSQL's PLpython extension to handle conversion of Python objects to PostgreSQL scalar data types.

## Definition
```c
typedef struct PLyObToScalar
{
    FmgrInfo    typfunc;        /* lookup info for type's input function */
    Oid         typioparam;     /* argument to pass to it */
} PLyObToScalar;
```

## Detailed Description
PLyObToScalar is a component structure used as part of the PLyObToDatum conversion system for handling scalar PostgreSQL data types. It encapsulates the necessary information to convert Python objects into scalar PostgreSQL values by storing the function manager information for the target type's input function and any required parameters. This structure is used within the discriminated union of PLyObToDatum when dealing with simple scalar types that require standard input function processing.

## Parameters / Member Variables
- `typfunc`: FmgrInfo structure containing cached lookup information for the PostgreSQL type's input function, which converts string representations to internal format
- `typioparam`: OID parameter that may need to be passed to the type's input function (used by some types like arrays for element type information)

## Dependencies
- Functions called/Symbols referenced:
  - [FmgrInfo](../F/FmgrInfo.md) (PostgreSQL function manager structure)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [PLyObToDatum](PLyObToDatum.md) (as union member 'scalar')
  - Various scalar conversion functions in plpy_typeio.c

## Notes and Other Information
PLyObToScalar is part of PostgreSQL's type conversion system for scalar values in PLpython. It works in conjunction with PostgreSQL's function manager (fmgr) system to efficiently convert Python objects to their corresponding PostgreSQL scalar representations. The typioparam field is used for types that require additional context during conversion, such as when the input function needs to know about element types or other metadata. This structure is only used for scalar types - arrays, tuples, domains, and types with transforms have their own specialized structures within the PLyObToDatum union.