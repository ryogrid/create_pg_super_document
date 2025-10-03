# findTypeInputFunction

## Location
[src/backend/commands/typecmds.c:1953-2015](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L1953-L2015)

## Overview
findTypeInputFunction locates and validates an appropriate input function for a PostgreSQL data type, ensuring it meets the required signature and behavioral constraints.

## Definition

```c
static Oid
findTypeInputFunction(List *procname, Oid typeOid)
```
## Detailed Description
This function performs comprehensive validation of type input functions by:

1. **Signature Resolution**: Searches for functions matching two valid input function signatures:
   - Single-argument form:  
   - Three-argument form:  (for types needing typioparam and typmod)

2. **Ambiguity Detection**: Reports an error if both signature forms exist for the same function name, preventing ambiguous function resolution

3. **Return Type Validation**: Ensures the input function returns the target type being defined, maintaining type system consistency

4. **Volatility Warning**: Issues a warning if the input function is marked as VOLATILE, since I/O functions should typically be STABLE or IMMUTABLE for system stability

The function follows PostgreSQL's convention that type input functions convert text representations to the internal type representation, and must be deterministic for proper catalog behavior.

## Parameters / Member Variables
- `*procname`: List of name components specifying the input function name (supports qualified names)
- `typeOid`: The OID of the type for which this input function is being located and validated
## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncName](../L/LookupFuncName.md) (searches for functions by name and argument signature)
  - [NameListToString](../N/NameListToString.md) (converts name list to string representation for error messages)
  - [func_signature_string](func_signature_string.md) (generates function signature strings for error reporting)
  - [get_func_rettype](../g/get_func_rettype.md) (retrieves function return type for validation)
  - [func_volatile](func_volatile.md) (checks function volatility classification)
  - PROVOLATILE_VOLATILE (volatility constant for comparison)
- Called from (representative examples):
  - [DefineType](../D/DefineType.md) (during type creation)
  - AlterTypeRecurseParams (during type modifications)

## Notes and Other Information
- Supports both traditional single-argument and extended three-argument input function signatures
- Three-argument form allows functions to handle type I/O parameters and type modifiers
- Throws ERRCODE_AMBIGUOUS_FUNCTION if multiple matching signatures exist
- Throws ERRCODE_UNDEFINED_FUNCTION if no matching function is found
- Issues warnings rather than errors for volatile functions to maintain backward compatibility
- Returns the OID of the validated input function for use in type catalog creation

## Simplified Source

```c
static Oid
findTypeInputFunction(List *procname, Oid typeOid)
{
    Oid argList[3];
    Oid procOid, procOid2;

    // Set up argument types - all input functions use INTERNAL types
    argList[0] = CSTRINGOID;  // String input
    argList[1] = OIDOID;      // Type I/O param
    argList[2] = INT4OID;     // Type modifier

    // Look for both 1-argument and 3-argument forms
    procOid = LookupFuncName(procname, 1, argList, true);   // (cstring)
    procOid2 = LookupFuncName(procname, 3, argList, true);  // (cstring, oid, int4)

    // Check for ambiguity - both forms shouldn't exist
    if (OidIsValid(procOid)) {
        if (OidIsValid(procOid2))
            ereport(ERROR, (errcode(ERRCODE_AMBIGUOUS_FUNCTION),
                           errmsg("type input function %s has multiple matches",
                                  NameListToString(procname))));
    } else {
        procOid = procOid2;
        // Error if neither form found
        if (!OidIsValid(procOid))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                           errmsg("function %s does not exist",
                                  func_signature_string(procname, 1, NIL, argList))));
    }

    // Validate return type matches target type
    if (get_func_rettype(procOid) != typeOid)
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                       errmsg("type input function %s must return type %s",
                              NameListToString(procname), format_type_be(typeOid))));

    // Warn about volatile functions (should be stable/immutable)
    if (func_volatile(procOid) == PROVOLATILE_VOLATILE)
        ereport(WARNING, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                         errmsg("type input function %s should not be volatile",
                                NameListToString(procname))));

    return procOid;
}
```