# domain_recv

## Location
[src/backend/utils/adt/domains.c:287-345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/domains.c#L287-L345)

## Overview
Binary input routine for any domain type that converts binary data to the domain's internal representation and validates domain constraints.

## Definition

```c
Datum
domain_recv(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as the universal binary input conversion function for all domain types in PostgreSQL. It is the binary equivalent of , handling binary format input instead of text format. The function follows the standard PostgreSQL binary type input function protocol.

The function performs operations similar to :
1. **Input validation**: Handles null inputs appropriately since the function is not strict
2. **Caching**: Uses function call context to cache DomainIOData between calls for efficiency  
3. **Base type conversion**: Delegates binary parsing to the underlying base type's receive function
4. **Constraint validation**: Applies all domain constraints to ensure the value is valid

The key difference from  is that it processes binary data using  rather than text data, and it calls  with the  parameter set to true.

## Parameters / Member Variables
The function uses the PostgreSQL function calling convention with:
- : StringInfo buffer containing binary data to be converted
- : OID of the domain type (typioparam)
- Binary input functions do not use error contexts (NULL passed to domain_check_input)

## Dependencies
- Functions called/Symbols referenced:
  - [DomainIOData](../D/DomainIOData.md) (struct type)
  - [domain_state_setup](domain_state_setup.md)
  - [ReceiveFunctionCall](../R/ReceiveFunctionCall.md)
  - [domain_check_input](domain_check_input.md)
  - PG_RETURN_DATUM
  - PG_FUNCTION_ARGS (macro)
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - PG_GETARG_OID
  - PG_RETURN_NULL
  - StringInfo (type)

- Called from (representative examples):
  - No direct references found (called via PostgreSQL's function manager system for binary input)

## Notes and Other Information
- The function is not strict, meaning it must explicitly handle null inputs
- Caches DomainIOData in fcinfo->flinfo->fn_extra for performance across multiple calls
- Handles domain type changes by recreating the cache when necessary
- Uses ReceiveFunctionCall instead of InputFunctionCallSafe for binary data processing
- Does not support error contexts (passes NULL to domain_check_input) unlike domain_in
- Returns null if the input buffer is null
- All domain constraints are validated after successful base type conversion
- The function follows PostgreSQL's standard binary type input function protocol
- Sets up the cache with binary=true flag to use binary I/O functions for the base type