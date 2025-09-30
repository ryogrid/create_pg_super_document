# lookup_fdw_handler_func

## Location
[src/backend/commands/foreigncmds.c:486-509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/foreigncmds.c#L486-L509)

## Overview
Converts a handler function name from the parser into an OID, validating that it has the correct return type for a foreign-data wrapper handler.

## Definition
```c
static Oid lookup_fdw_handler_func(DefElem *handler)
```

## Detailed Description
This utility function processes handler function specifications from SQL DDL commands (like CREATE FOREIGN DATA WRAPPER) by converting the function name list into a function OID. It performs critical validation to ensure the specified function is suitable as an FDW handler by checking that it takes no arguments and returns the fdw_handler type. The function follows PostgreSQL's pattern for DDL option processing by handling NULL inputs gracefully and providing detailed error messages when validation fails.

## Parameters / Member Variables
- `handler`: DefElem structure containing the handler function name specification from the parser, may be NULL if no handler is specified

## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncName](../L/LookupFuncName.md): Resolve function name list to OID with signature checking
  - [get_func_rettype](../g/get_func_rettype.md): Retrieve the return type OID of a function
  - [NameListToString](../N/NameListToString.md): Convert function name list to string for error messages
  - FDW_HANDLEROID: Constant representing the fdw_handler type OID
- Called from (representative examples):
  - [parse_func_options](../p/parse_func_options.md): Parser helper for processing FDW creation options

## Notes and Other Information
- Returns InvalidOid if handler is NULL or handler->arg is NULL, indicating no handler specified
- Validates that handler functions take exactly zero arguments
- Enforces that handler functions must return fdw_handler type
- Used during CREATE FOREIGN DATA WRAPPER and ALTER FOREIGN DATA WRAPPER processing
- Part of PostgreSQL's type safety mechanisms for FDW infrastructure
- Error messages include the full function name for user clarity
- Static function, only used internally within foreigncmds.c module

## Simplified Source

```c
static Oid
lookup_fdw_handler_func(DefElem *handler)
{
    Oid handlerOid;

    // Return InvalidOid if no handler specified
    if (handler == NULL || handler->arg == NULL)
        return InvalidOid;

    // Look up function with zero arguments (handlers take no parameters)
    handlerOid = LookupFuncName((List *) handler->arg, 0, NULL, false);

    // Validate that handler returns fdw_handler type
    if (get_func_rettype(handlerOid) != FDW_HANDLEROID)
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("function %s must return type %s",
                        NameListToString((List *) handler->arg), "fdw_handler")));

    return handlerOid;
}
```