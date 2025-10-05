# domain_in

## Location
[src/backend/utils/adt/domains.c:227-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/domains.c#L227-L286)

## Overview
Input routine for any domain type that converts string input to the domain's internal representation and validates domain constraints.

## Definition

```c
Datum
domain_in(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as the universal text input conversion function for all domain types in PostgreSQL. It follows the standard PostgreSQL type input function protocol, taking a string representation and converting it to the domain's internal Datum format.

The function performs several key operations:
1. **Input validation**: Handles null inputs appropriately since the function is not strict
2. **Caching**: Uses function call context to cache DomainIOData between calls for efficiency
3. **Base type conversion**: Delegates the actual parsing to the underlying base type's input function
4. **Constraint validation**: Applies all domain constraints to ensure the value is valid

The function is designed to be called through PostgreSQL's function manager (fmgr) system and follows the PG_FUNCTION_ARGS calling convention.

## Parameters / Member Variables
The function uses the PostgreSQL function calling convention with:
- : Input string to be converted
- : OID of the domain type (typioparam)
- : Error context for soft error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [DomainIOData](../D/DomainIOData.md) (struct type)
  - [domain_state_setup](domain_state_setup.md)
  - [InputFunctionCallSafe](../I/InputFunctionCallSafe.md)
  - [domain_check_input](domain_check_input.md)
  - PG_RETURN_DATUM
  - PG_FUNCTION_ARGS (macro)
  - PG_ARGISNULL
  - PG_GETARG_CSTRING
  - PG_GETARG_OID
  - PG_RETURN_NULL

- Called from (representative examples):
  - No direct references found (called via PostgreSQL's function manager system)

## Notes and Other Information
- The function is not strict, meaning it must explicitly handle null inputs
- Caches DomainIOData in fcinfo->flinfo->fn_extra for performance across multiple calls
- Handles domain type changes by recreating the cache when necessary
- Uses InputFunctionCallSafe for safe conversion that supports error contexts
- Returns null if the input string is null or if base type conversion fails
- All domain constraints are validated after successful base type conversion
- The function follows PostgreSQL's standard type input function protocol

## Simplified Source

```c
Datum domain_in(PG_FUNCTION_ARGS) {
    char *string;
    Oid domainType;
    Node *escontext = fcinfo->context;
    DomainIOData *my_extra;
    Datum value;

    // Handle null inputs (function is not strict)
    if (PG_ARGISNULL(0)) {
        string = NULL;
    } else {
        string = PG_GETARG_CSTRING(0);
    }

    if (PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    }
    domainType = PG_GETARG_OID(1);

    // Set up or reuse cached domain information
    my_extra = (DomainIOData *) fcinfo->flinfo->fn_extra;
    if (my_extra == NULL || my_extra->domain_type != domainType) {
        my_extra = domain_state_setup(domainType, false, fcinfo->flinfo->fn_mcxt);
        fcinfo->flinfo->fn_extra = (void *) my_extra;
    }

    // Convert input using base type's input function
    if (!InputFunctionCallSafe(&my_extra->proc, string, my_extra->typioparam,
                               my_extra->typtypmod, escontext, &value)) {
        PG_RETURN_NULL();
    }

    // Validate domain constraints
    domain_check_input(value, (string == NULL), my_extra, escontext);

    if (string == NULL) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_DATUM(value);
    }
}
```