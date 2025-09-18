# findTypeTypmodoutFunction

## Location
src/backend/commands/typecmds.c: 2174 - 2207

## Overview
Validates and retrieves the OID of a type's typmod output function, which converts internal integer type modifier representations back to human-readable string format.

## Definition
```c
static Oid findTypeTypmodoutFunction(List *procname)
```

## Detailed Description
This function locates and validates a type modifier output function during type definition or modification. Type modifier output functions are responsible for converting PostgreSQL's internal integer representation of type modifiers back into human-readable string format for display purposes. These functions are the reverse counterpart of typmod input functions and are essential for types that support parameterization, enabling proper display of type specifications in system catalogs, error messages, and user interfaces. The function ensures the specified function meets PostgreSQL's requirements for typmod output functions.

## Parameters / Member Variables
- `procname`: A list representing the qualified name of the typmod output function to look up

## Dependencies
- Functions called/Symbols referenced:
  - LookupFuncName: Looks up function by name with specified argument types
  - func_signature_string: Creates a string representation of function signature for error messages
  - get_func_rettype: Retrieves the return type OID of a function
  - NameListToString: Converts a name list to string format for display
  - func_volatile: Checks the volatility category of a function
  - PROVOLATILE_VOLATILE: Constant representing volatile function category
- Called from (representative examples):
  - DefineType: When creating a new data type with type modifiers
  - AlterType: When modifying an existing data type's modifier functions
  - AlterTypeRecurseParams: When modifying type parameters

## Notes and Other Information
- Typmod output functions must take exactly one int4 argument and return cstring
- The int4 argument contains the internal representation of the type modifier
- The returned cstring should be a human-readable representation suitable for display
- The function issues an error if the specified function doesn't exist or has wrong signature
- A warning is issued if the function is marked as volatile, as typmod functions should typically be immutable
- This works in conjunction with findTypeTypmodinFunction to provide complete type modifier support
- Used when displaying type information in pg_dump, \d commands, and error messages