# findTypeSubscriptingFunction

## Location
[src/backend/commands/typecmds.c:2235-2281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L2235-L2281)

## Overview
This function validates and retrieves the OID of a user-specified subscripting function for a PostgreSQL data type, ensuring it meets the required signature constraints and is not a reserved array handler function.

## Definition
```c
static Oid findTypeSubscriptingFunction(List *procname, Oid typeOid)
```

## Detailed Description
The `findTypeSubscriptingFunction` is a static helper function used during type definition and modification operations to validate subscripting support functions. Subscripting functions enable custom types to support array-like indexing operations (e.g., `mytype[index]`) by providing the necessary infrastructure for element access, assignment, and other subscripting operations.

The function performs several critical validations:
1. Ensures the specified function exists and takes exactly one INTERNAL argument
2. Verifies that the function returns an INTERNAL value
3. Explicitly prohibits the use of `array_subscript_handler()`, which is reserved for auto-generated array types

This validation ensures that only properly formed, user-appropriate subscripting functions can be associated with custom types, maintaining type safety and preventing misuse of internal PostgreSQL mechanisms.

## Parameters / Member Variables
- `procname`: A List containing the qualified name components of the subscripting function to validate
- `typeOid`: The OID of the type for which this subscripting function is being set (currently unused but available for future enhancements)

## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncName](../L/LookupFuncName.md): Locates the function by name and signature
  - [func_signature_string](func_signature_string.md): Formats function signature for error messages
  - [get_func_rettype](../g/get_func_rettype.md): Retrieves the return type of a function
  - [NameListToString](../N/NameListToString.md): Converts qualified name list to string representation
- Called from:
  - [DefineType](../D/DefineType.md): During creation of new user-defined types with subscripting support
  - [AlterType](../A/AlterType.md): When modifying existing type properties to add/change subscripting
  - AlterTypeRecurseParams: As part of recursive type alteration operations

## Notes and Other Information
- Subscripting functions must have the signature `function_name(internal) returns internal`
- The INTERNAL argument is required for type safety but may not be used by the function implementation
- The returned INTERNAL value represents the subscripting handler structure that PostgreSQL uses internally
- The explicit prohibition of `array_subscript_handler()` prevents users from accidentally breaking the distinction between user-defined types and system-managed array types
- This function is part of PostgreSQL's extensible type system, enabling custom types to implement sophisticated indexing behaviors
- The function is located in src/backend/commands/typecmds.c:2235-2281