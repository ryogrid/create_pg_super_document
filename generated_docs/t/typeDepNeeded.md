# typeDepNeeded

## Location
[src/backend/commands/opclasscmds.c:1675-1724](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L1675-L1724)

## Overview
Determines whether a pg_amop or pg_amproc catalog entry requires an explicit dependency on its lefttype or righttype to maintain proper referential integrity.

## Definition

```c
static bool
typeDepNeeded(Oid typid, OpFamilyMember *member)
```
## Detailed Description
This function implements an optimization strategy for dependency management in PostgreSQL's operator family system. It analyzes whether an explicit dependency between a catalog entry (operator or support function) and a data type is necessary. The function returns false (no dependency needed) when the entry already has an indirect dependency via its referenced operator or function, which is typically the case for operators but may not be true for support functions. This optimization reduces unnecessary dependency entries and improves system performance by avoiding redundant dependency tracking.

## Parameters / Member Variables
- `typid`: Object identifier of the data type being checked for dependency requirements
- `*member`: Pointer to OpFamilyMember structure containing information about the operator or support function
## Dependencies
- Functions called/Symbols referenced:
  - [IsPinnedObject](../I/IsPinnedObject.md)
  - [get_func_signature](../g/get_func_signature.md)
  - [pfree](../p/pfree.md)
  - [op_input_types](../o/op_input_types.md)
- Called from (representative examples):
  - [storeOperators](../s/storeOperators.md) (src/backend/commands/opclasscmds.c:1511, 1523)
  - [storeProcedures](../s/storeProcedures.md) (src/backend/commands/opclasscmds.c:1635, 1647)

## Notes and Other Information
- Returns false immediately if the type is a pinned object (built-in types), as these don't require dependency tracking
- For functions (member->is_func == true), checks if the type appears in the function's argument list
- For operators (member->is_func == false), checks if the type matches either the left or right operand type
- The function performs a layering violation optimization by checking pinned objects directly rather than relying on recordDependencyOn to ignore the request
- Memory allocated by get_func_signature for the argtypes array is properly freed with pfree()
- This optimization is crucial for performance in large databases with many operator families and custom types

## Simplified Source

```c
static bool
typeDepNeeded(Oid typid, OpFamilyMember *member)
{
    // Pinned types don't need dependencies
    if (IsPinnedObject(TypeRelationId, typid))
        return false;

    // Check if type appears in function/operator signature
    if (member->is_func) {
        // For functions, check argument types
        Oid *argtypes;
        int nargs;

        get_func_signature(member->object, &argtypes, &nargs);
        for (int i = 0; i < nargs; i++) {
            if (typid == argtypes[i]) {
                pfree(argtypes);
                return false; // Type found in signature, no dependency needed
            }
        }
        pfree(argtypes);
    } else {
        // For operators, check left and right operand types
        Oid lefttype, righttype;
        op_input_types(member->object, &lefttype, &righttype);
        if (typid == lefttype || typid == righttype)
            return false; // Type matches operand, no dependency needed
    }

    return true; // Explicit dependency needed
}
```