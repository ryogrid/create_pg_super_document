# lookup_am_handler_func

## Location
src/backend/commands/amcmds.c: 234 - 269

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
  - LookupFuncName
  - AMTYPE_INDEX
  - AMTYPE_TABLE
  - INDEX_AM_HANDLEROID
  - TABLE_AM_HANDLEROID
  - get_func_rettype
  - get_func_name
  - format_type_extended
  - ereport
  - errcode
  - errmsg
  - elog
  - ERROR
  - ERRCODE_UNDEFINED_FUNCTION
  - ERRCODE_WRONG_OBJECT_TYPE

- Called from (representative examples):
  - CreateAccessMethod (src/backend/commands/amcmds.c:78)

## Notes and Other Information
- This is a static function, only accessible within amcmds.c
- Always returns a valid function OID or throws an ERROR - never returns InvalidOid
- Enforces that handler functions must have exactly one parameter of type 'internal'
- Validates return type compatibility: INDEX access methods require index_am_handler return type, TABLE access methods require table_am_handler return type
- Part of the CREATE ACCESS METHOD command implementation
- Critical for ensuring access method handler functions conform to PostgreSQL's internal API requirements
- Function names are resolved using the standard PostgreSQL function lookup mechanism, supporting schema-qualified names