# findTypeTypmodoutFunction

## Location
[src/backend/commands/typecmds.c:2174-2207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L2174-L2207)

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
- Typmod output functions must take exactly one int4 argument and return cstring
- The int4 argument contains the internal representation of the type modifier
- The returned cstring should be a human-readable representation suitable for display
- The function issues an error if the specified function doesn't exist or has wrong signature
- A warning is issued if the function is marked as volatile, as typmod functions should typically be immutable
- This works in conjunction with findTypeTypmodinFunction to provide complete type modifier support
- Used when displaying type information in pg_dump, \d commands, and error messages

## Simplified Source

```c
static Oid findTypeTypmodoutFunction(List *procname) {
    Oid argList[1] = { INT4OID };

    // Look up function with signature: (int4) -> cstring
    Oid procOid = LookupFuncName(procname, 1, argList, true);

    if (!OidIsValid(procOid))
        ereport(ERROR, "function %s does not exist",
                func_signature_string(procname, 1, NIL, argList));

    // Verify function returns cstring
    if (get_func_rettype(procOid) != CSTRINGOID)
        ereport(ERROR, "typmod_out function %s must return type cstring",
                NameListToString(procname));

    // Warn if volatile (should be immutable)
    if (func_volatile(procOid) == PROVOLATILE_VOLATILE)
        ereport(WARNING, "type modifier output function %s should not be volatile",
                NameListToString(procname));

    return procOid;
}
```