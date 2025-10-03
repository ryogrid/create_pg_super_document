# findTypeOutputFunction

## Location
[src/backend/commands/typecmds.c:2016-2050](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L2016-L2050)

## Overview
Validates and retrieves the OID of a type's output function, ensuring it meets PostgreSQL's requirements for converting internal type representation to external string format.

## Definition

```c
static Oid
findTypeOutputFunction(List *procname, Oid typeOid)
```
## Detailed Description
This function is responsible for locating and validating a type output function during type definition or modification. Type output functions are critical components that convert PostgreSQL's internal binary representation of a data type to its external string representation (cstring). The function performs several validation checks to ensure the specified function meets PostgreSQL's strict requirements for output functions, including proper signature validation and return type verification.

## Parameters / Member Variables
- `*procname`: A list representing the qualified name of the output function to look up
- `typeOid`: The OID of the data type for which this will serve as the output function
## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncName](../L/LookupFuncName.md): Looks up function by name with specified argument types
  - [func_signature_string](func_signature_string.md): Creates a string representation of function signature for error messages  
  - [get_func_rettype](../g/get_func_rettype.md): Retrieves the return type OID of a function
  - [NameListToString](../N/NameListToString.md): Converts a name list to string format for display
  - [func_volatile](func_volatile.md): Checks the volatility category of a function
  - PROVOLATILE_VOLATILE: Constant representing volatile function category
- Called from (representative examples):
  - [DefineType](../D/DefineType.md): When creating a new data type
  - AlterTypeRecurseParams: When modifying type parameters

## Notes and Other Information
- Output functions must take exactly one argument of the target type and return cstring
- The function issues an error if the specified function doesn't exist or has wrong return type
- A warning is issued (not an error) if the function is marked as volatile, as output functions should typically be stable or immutable
- This is part of PostgreSQL's type system infrastructure that ensures type safety and proper data conversion

## Simplified Source

```c
static Oid
findTypeOutputFunction(List *procname, Oid typeOid)
{
    Oid argList[1];
    Oid procOid;

    // Output functions take one argument of the target type
    argList[0] = typeOid;

    // Look up the function with single argument signature
    procOid = LookupFuncName(procname, 1, argList, true);
    if (!OidIsValid(procOid))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                       errmsg("function %s does not exist",
                              func_signature_string(procname, 1, NIL, argList))));

    // Validate return type is cstring
    if (get_func_rettype(procOid) != CSTRINGOID)
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                       errmsg("type output function %s must return type %s",
                              NameListToString(procname), "cstring")));

    // Warn about volatile functions (should be stable/immutable)
    if (func_volatile(procOid) == PROVOLATILE_VOLATILE)
        ereport(WARNING, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                         errmsg("type output function %s should not be volatile",
                                NameListToString(procname))));

    return procOid;
}
```