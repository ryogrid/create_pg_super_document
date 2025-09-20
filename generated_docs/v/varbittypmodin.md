# varbittypmodin

## Location
[src/backend/utils/adt/varbit.c:774-781](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L774-L781)

## Overview
Type modifier input function for the variable-length bit string (VARBIT) data type that validates and processes length specifications.

## Definition

```c
Datum
varbittypmodin(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL type modifier input function specifically for the VARBIT (variable-length bit string) data type. It serves as a wrapper around the common  function, providing type-specific error messages and validation for variable-length bit strings. When a VARBIT type is declared with a length specification (e.g., VARBIT(10)), this function processes and validates that specification during type creation or casting operations.

The function extracts the array of type modifiers passed from the SQL parser and delegates the actual validation logic to , which ensures the length specification is within valid bounds (at least 1 and not exceeding MaxAttrSize * BITS_PER_BYTE).

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  -  (ArrayType*): Array of type modifiers containing the length specification for the VARBIT type

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract ArrayType argument from function args
  - : Common validation logic for bit string type modifiers
  - : Macro to return a 32-bit integer result

- Called from (representative examples):
  - PostgreSQL type system during type creation and casting operations
  - SQL parser when processing VARBIT type declarations

## Notes and Other Information
- This function is part of the PostgreSQL type system infrastructure for bit string types
- It specifically handles VARBIT types, while  handles fixed-length BIT types
- The function validates that length specifications are reasonable (between 1 and the maximum allowed)
- Returns the validated length as a type modifier value that gets stored with the type information
- Located in src/backend/utils/adt/varbit.c:774-781