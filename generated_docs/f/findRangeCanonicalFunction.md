# findRangeCanonicalFunction

## Location
[src/backend/commands/typecmds.c:2321-2361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L2321-L2361)

## Overview
This function validates and retrieves the OID of a user-specified canonical function for a PostgreSQL range type, ensuring it meets signature, immutability, and permission requirements.

## Definition
```c
static Oid findRangeCanonicalFunction(List *procname, Oid typeOid)
```

## Detailed Description
The `findRangeCanonicalFunction` is a static helper function used during range type definition and modification to validate canonical functions. A canonical function for a range type is responsible for converting any range value into a standardized "canonical" form, which ensures consistent representation and enables efficient operations and comparisons.

The function performs comprehensive validation:
1. **Signature validation**: Ensures the function takes and returns the range type being defined
2. **Immutability requirement**: Verifies the function is marked as IMMUTABLE, which is crucial for consistency and indexing
3. **Permission checking**: Confirms that the range type creator has EXECUTE permission on the function

The immutability requirement is particularly important because canonical functions must produce consistent results for the same input across different sessions and times, which is essential for index operations and query optimization.

## Parameters / Member Variables
- `procname`: A List containing the qualified name components of the canonical function to validate
- `typeOid`: The OID of the range type for which this canonical function is being set (used for signature validation)

## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncName](../L/LookupFuncName.md): Locates the function by name and signature
  - [func_signature_string](func_signature_string.md): Formats function signature for error messages
  - [get_func_rettype](../g/get_func_rettype.md): Retrieves the return type of a function
  - [func_volatile](func_volatile.md): Gets the volatility classification of a function
  - [object_aclcheck](../o/object_aclcheck.md): Checks access permissions for database objects
  - [aclcheck_error](../a/aclcheck_error.md): Reports permission-related errors
  - [get_func_name](../g/get_func_name.md): Retrieves function name for error reporting
- Called from:
  - [DefineRange](../D/DefineRange.md): During creation of new range types with canonical functions
  - AlterTypeRecurseParams: As part of recursive type alteration operations

## Notes and Other Information
- Canonical functions must have the signature `function_name(rangetype) returns rangetype`
- The IMMUTABLE volatility requirement ensures the function can be safely used in indexes and for query optimization
- Permission checking prevents security issues where users might reference functions they cannot execute
- Canonical functions are optional for range types but can improve performance and ensure consistent representation
- Common use cases include normalizing different representations of equivalent ranges (e.g., `[1,3)` vs `[1,2]`)
- This function is part of PostgreSQL's extensible range type system
- Located in src/backend/commands/typecmds.c:2321-2361

## Simplified Source

```c
static Oid
findRangeCanonicalFunction(List *procname, Oid typeOid)
{
    Oid argList[1] = {typeOid};

    // Look up the function with range type as argument
    Oid procOid = LookupFuncName(procname, 1, argList, true);
    if (!OidIsValid(procOid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("function %s does not exist",
                        func_signature_string(procname, 1, NIL, argList))));

    // Validate return type matches range type
    if (get_func_rettype(procOid) != typeOid)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("range canonical function %s must return range type",
                        func_signature_string(procname, 1, NIL, argList))));

    // Ensure function is immutable for consistency
    if (func_volatile(procOid) != PROVOLATILE_IMMUTABLE)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("range canonical function %s must be immutable",
                        func_signature_string(procname, 1, NIL, argList))));

    // Check execute permission
    AclResult aclresult = object_aclcheck(ProcedureRelationId, procOid, GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(procOid));

    return procOid;
}
```