# findTypeReceiveFunction

## Location
[src/backend/commands/typecmds.c:2051-2104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L2051-L2104)

## Overview
Validates and retrieves the OID of a type's receive function, which converts external binary representation to internal format, supporting both single-argument and three-argument function signatures.

## Definition
```c
static Oid findTypeReceiveFunction(List *procname, Oid typeOid)
```

## Detailed Description
This function locates and validates a type receive function during type definition or modification. Type receive functions are essential components that convert PostgreSQL's external binary representation of a data type to its internal binary format. The function supports two distinct signatures: a simple single-argument version taking only INTERNAL, and a more complex three-argument version that also accepts typioparam OID and typmod parameters. The function performs ambiguity checking to ensure only one form exists and validates that the function returns the correct type.

## Parameters / Member Variables
- `procname`: A list representing the qualified name of the receive function to look up
- `typeOid`: The OID of the data type for which this will serve as the receive function

## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncName](../L/LookupFuncName.md): Looks up function by name with specified argument types (called twice for different signatures)
  - [NameListToString](../N/NameListToString.md): Converts a name list to string format for display
  - [func_signature_string](func_signature_string.md): Creates a string representation of function signature for error messages
  - [get_func_rettype](../g/get_func_rettype.md): Retrieves the return type OID of a function
  - [format_type_be](format_type_be.md): Formats a type OID as a readable type name
  - [func_volatile](func_volatile.md): Checks the volatility category of a function
  - PROVOLATILE_VOLATILE: Constant representing volatile function category
- Called from (representative examples):
  - [DefineType](../D/DefineType.md): When creating a new data type
  - [AlterType](../A/AlterType.md): When modifying an existing data type
  - AlterTypeRecurseParams: When modifying type parameters

## Notes and Other Information
- Receive functions can have either 1 argument (internal) or 3 arguments (internal, oid, int4)
- The function reports an error if both signature forms exist simultaneously to avoid ambiguity
- The receive function must return exactly the target type being defined
- A warning is issued if the function is marked as volatile, as receive functions should typically be stable or immutable
- This complements the type's send function to provide binary I/O capabilities for network transmission and storage

## Simplified Source

```c
static Oid findTypeReceiveFunction(List *procname, Oid typeOid) {
    Oid argList[3] = { INTERNALOID, OIDOID, INT4OID };

    // Look for both 1-arg and 3-arg signatures
    Oid procOid1 = LookupFuncName(procname, 1, argList, true);
    Oid procOid3 = LookupFuncName(procname, 3, argList, true);

    // Check for ambiguity (both signatures exist)
    if (OidIsValid(procOid1)) {
        if (OidIsValid(procOid3))
            ereport(ERROR, "type receive function %s has multiple matches",
                    NameListToString(procname));
        procOid = procOid1;
    } else {
        procOid = procOid3;
        if (!OidIsValid(procOid))
            ereport(ERROR, "function %s does not exist",
                    func_signature_string(procname, 1, NIL, argList));
    }

    // Verify function returns the target type
    if (get_func_rettype(procOid) != typeOid)
        ereport(ERROR, "type receive function %s must return type %s",
                NameListToString(procname), format_type_be(typeOid));

    // Warn if volatile (should be stable/immutable)
    if (func_volatile(procOid) == PROVOLATILE_VOLATILE)
        ereport(WARNING, "type receive function %s should not be volatile",
                NameListToString(procname));

    return procOid;
}
```