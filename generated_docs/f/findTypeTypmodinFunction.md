# findTypeTypmodinFunction

## Location
src/backend/commands/typecmds.c: 2140 - 2173

## Overview
Validates and retrieves the OID of a type's typmod input function, which converts string representations of type modifiers to internal integer format.

## Definition
```c
static Oid findTypeTypmodinFunction(List *procname)
```

## Detailed Description
This function locates and validates a type modifier input function during type definition or modification. Type modifier input functions are responsible for parsing and converting string representations of type modifiers (such as length specifications in VARCHAR(50)) into PostgreSQL's internal integer representation. These functions are essential for types that support parameterization, allowing users to specify constraints or formatting options that affect the type's behavior. The function ensures the specified function meets PostgreSQL's strict requirements for typmod input functions.

## Parameters / Member Variables
- `procname`: A list representing the qualified name of the typmod input function to look up

## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncName](../L/LookupFuncName.md): Looks up function by name with specified argument types
  - [func_signature_string](func_signature_string.md): Creates a string representation of function signature for error messages
  - [get_func_rettype](../g/get_func_rettype.md): Retrieves the return type OID of a function
  - [NameListToString](../N/NameListToString.md): Converts a name list to string format for display
  - [func_volatile](func_volatile.md): Checks the volatility category of a function
  - PROVOLATILE_VOLATILE: Constant representing volatile function category
- Called from (representative examples):
  - [DefineType](../D/DefineType.md): When creating a new data type with type modifiers
  - [AlterType](../A/AlterType.md): When modifying an existing data type's modifier functions
  - AlterTypeRecurseParams: When modifying type parameters

## Notes and Other Information
- Typmod input functions must take exactly one cstring[] argument and return int4 (integer)
- The cstring[] argument contains the parsed type modifier values from the SQL type declaration
- The function issues an error if the specified function doesn't exist or has wrong signature
- A warning is issued if the function is marked as volatile, as typmod functions should typically be immutable
- This works in conjunction with findTypeTypmodoutFunction to provide complete type modifier support
- Type modifiers are commonly used for types like VARCHAR(n), DECIMAL(precision,scale), etc.