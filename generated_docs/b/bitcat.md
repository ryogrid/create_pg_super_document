# bitcat

## Location
[src/backend/utils/adt/varbit.c:968-976](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L968-L976)

## Overview
The bitcat function provides concatenation functionality for bit strings in PostgreSQL, serving as a wrapper function for the internal bit_catenate operation.

## Definition

```c
Datum
bitcat(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL SQL-callable function that concatenates two bit strings (VarBit types). It extracts two VarBit arguments from the function call arguments, passes them to the internal bit_catenate function for the actual concatenation work, and returns the result as a VarBit datum. The function follows PostgreSQL's standard function calling convention using the PG_FUNCTION_ARGS macro.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument: VarBit pointer (arg1) - the first bit string to concatenate
  - Second argument: VarBit pointer (arg2) - the second bit string to concatenate

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P (extracts VarBit arguments)
  - [bit_catenate](bit_catenate.md) (performs actual concatenation)
  - PG_RETURN_VARBIT_P (returns VarBit result)
- Called from:
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- This function is defined in src/backend/utils/adt/varbit.c at lines 968-976
- Acts as a thin wrapper around the bit_catenate function
- Follows PostgreSQL's V1 calling convention for SQL-callable functions
- The actual concatenation logic is implemented in the bit_catenate helper function