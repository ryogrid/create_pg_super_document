# lookup_am_handler_func

## Location
[src/backend/commands/amcmds.c:234-269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/amcmds.c#L234-L269)

## Overview
Resolves and validates an access method handler function name to its OID, ensuring the function signature matches the expected access method type.

## Definition
```c
static Oid lookup_am_handler_func(List *handler_name, char amtype)
```

## Detailed Description
This static function performs comprehensive validation of access method handler functions during access method creation. It takes a qualified function name and access method type, then:

1. Resolves the function name to its OID using the PostgreSQL function lookup mechanism
2. Validates that the function accepts exactly one argument of type 'internal'
3. Verifies that the function's return type matches the expected handler type for the given access method type
4. Returns the validated function OID or raises an error if validation fails

The function enforces PostgreSQL's access method handler function contract, which requires specific signatures for different access method types (INDEX vs TABLE). Handler functions must return either index_am_handler or table_am_handler types depending on the access method type.

## Parameters / Member Variables
- `handler_name`: A List containing the qualified name components of the handler function (e.g., schema and function name)
- `amtype`: A single character representing the access method type (AMTYPE_INDEX or AMTYPE_TABLE)

## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncName](../L/LookupFuncName.md)
  - AMTYPE_INDEX
  - AMTYPE_TABLE
  - INDEX_AM_HANDLEROID
  - TABLE_AM_HANDLEROID
  - [get_func_rettype](../g/get_func_rettype.md)
  - [get_func_name](../g/get_func_name.md)
  - [format_type_extended](../f/format_type_extended.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - elog
  - ERROR
  - ERRCODE_UNDEFINED_FUNCTION
  - ERRCODE_WRONG_OBJECT_TYPE

- Called from (representative examples):
  - [CreateAccessMethod](../C/CreateAccessMethod.md) (src/backend/commands/amcmds.c:78)

## Notes and Other Information
- This is a static function, only accessible within amcmds.c
- Always returns a valid function OID or throws an ERROR - never returns InvalidOid
- Enforces that handler functions must have exactly one parameter of type 'internal'
- Validates return type compatibility: INDEX access methods require index_am_handler return type, TABLE access methods require table_am_handler return type
- Part of the CREATE ACCESS METHOD command implementation
- Critical for ensuring access method handler functions conform to PostgreSQL's internal API requirements
- Function names are resolved using the standard PostgreSQL function lookup mechanism, supporting schema-qualified names

## Simplified Source
```c
static Oid
lookup_am_handler_func(List *handler_name, char amtype)
{
    Oid handlerOid;
    Oid funcargtypes[1] = {INTERNALOID};
    Oid expectedType;

    // Check if handler name is provided
    if (handler_name == NIL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                       errmsg("handler function is not specified")));

    // Look up the function (must take one 'internal' argument)
    handlerOid = LookupFuncName(handler_name, 1, funcargtypes, false);

    // Determine expected return type based on access method type
    switch (amtype)
    {
        case AMTYPE_INDEX:
            expectedType = INDEX_AM_HANDLEROID;
            break;
        case AMTYPE_TABLE:
            expectedType = TABLE_AM_HANDLEROID;
            break;
        default:
            elog(ERROR, "unrecognized access method type \"%c\"", amtype);
    }

    // Validate return type matches expected handler type
    if (get_func_rettype(handlerOid) != expectedType)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("function %s must return type %s",
                             get_func_name(handlerOid),
                             format_type_extended(expectedType, -1, 0))));

    return handlerOid;
}
```