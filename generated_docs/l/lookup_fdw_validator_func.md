# lookup_fdw_validator_func

## Location
[src/backend/commands/foreigncmds.c:510-528](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L510-L528)

## Overview
Converts a validator function name from the parser into an OID, ensuring it has the correct signature for a foreign-data wrapper validator function.

## Definition
```c
static Oid lookup_fdw_validator_func(DefElem *validator)
```

## Detailed Description
This utility function processes validator function specifications from SQL DDL commands by converting the function name list into a function OID. Unlike handler functions, validator functions must accept specific arguments: a text array containing options and an OID representing the catalog relation. The function enforces the required signature by explicitly specifying the expected argument types during function lookup, but does not validate the return type since validator return values are ignored by the FDW infrastructure.

## Parameters / Member Variables
- `validator`: DefElem structure containing the validator function name specification from the parser, may be NULL if no validator is specified

## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncName](../L/LookupFuncName.md): Resolve function name list to OID with signature checking using specific argument types
  - TEXTARRAYOID: Type OID constant for text array type
  - OIDOID: Type OID constant for OID type
- Called from (representative examples):
  - [parse_func_options](../p/parse_func_options.md): Parser helper for processing FDW creation options

## Notes and Other Information
- Returns InvalidOid if validator is NULL or validator->arg is NULL, indicating no validator specified
- Enforces exact signature: validator functions must take (text[], oid) arguments
- Does not validate return type since validator return values are ignored
- Used during CREATE FOREIGN DATA WRAPPER and ALTER FOREIGN DATA WRAPPER processing
- Validator functions are called to validate options before storing them in catalogs
- Static function, only used internally within foreigncmds.c module
- Part of PostgreSQL's option validation infrastructure for FDW management

## Simplified Source

```c
static Oid lookup_fdw_validator_func(DefElem *validator) {
    // Return InvalidOid if no validator specified
    if (validator == NULL || validator->arg == NULL)
        return InvalidOid;

    // Set up expected argument types: validators take text[], oid
    Oid funcargtypes[2];
    funcargtypes[0] = TEXTARRAYOID;  // options array
    funcargtypes[1] = OIDOID;        // catalog relation OID

    // Look up function by name with required signature
    return LookupFuncName((List *) validator->arg, 2, funcargtypes, false);
}
```