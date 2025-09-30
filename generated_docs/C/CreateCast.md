# CreateCast

## Location
[src/backend/commands/functioncmds.c:1521-1783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L1521-L1783)

## Overview
Implements the CREATE CAST command to define type conversion operations between source and target data types using various coercion methods.

## Definition

```c
struct;
```
## Detailed Description
CreateCast processes CREATE CAST statements to establish type conversion mechanisms in PostgreSQL. The function performs extensive validation and supports three coercion methods:

1. **Function-based casts** - Use a conversion function with strict parameter validation
2. **Input/Output casts** - Convert via text representation using type I/O functions  
3. **Binary-compatible casts** - Direct memory reinterpretation (superuser only)

Key validation steps include:
- Permission checks requiring ownership or usage rights on both types
- Pseudo-type rejection for source and target types
- Domain type warnings (allowed but discouraged for compatibility)
- Cast function signature validation (1-3 parameters with specific types)
- Binary compatibility checks for physical type representation
- Restriction of binary casts for composite, array, range, enum, and domain types
- Coercion context mapping from SQL syntax to internal codes

The function delegates actual catalog insertion to CastCreate after completing all validation.

## Parameters / Member Variables
- : CreateCastStmt structure containing source type, target type, optional cast function, coercion context, and method specification

## Dependencies
- Functions called/Symbols referenced:
  - [typenameTypeId](../t/typenameTypeId.md)
  - [get_typtype](../g/get_typtype.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [LookupFuncWithArgs](../L/LookupFuncWithArgs.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [IsBinaryCoercibleWithCast](../I/IsBinaryCoercibleWithCast.md)
  - [superuser](../s/superuser.md)
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - [get_element_type](../g/get_element_type.md)
  - [CastCreate](CastCreate.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility.c:1720)

## Notes and Other Information
- Enforces superuser requirement for binary-compatible casts due to crash risk from erroneous casts
- Validates cast function signatures: first parameter must match/be coercible from source type, optional second parameter must be integer, optional third parameter must be boolean
- Includes commented-out volatility check (#ifdef NOT_USED) that was disabled to maintain compatibility with user-defined types
- Prevents self-casts except for length coercion functions (multi-argument functions)
- Maps SQL coercion contexts (IMPLICIT, ASSIGNMENT, EXPLICIT) to internal character codes
- Physical compatibility checks ensure matching length, pass-by-value semantics, and alignment between types for binary casts

## Simplified Source

```c
ObjectAddress
CreateCast(CreateCastStmt *stmt)
{
    Oid sourcetypeid, targettypeid;
    char sourcetyptype, targettyptype;
    Oid funcid = InvalidOid;
    char castcontext, castmethod;
    ObjectAddress myself;

    // Get source and target type OIDs
    sourcetypeid = typenameTypeId(NULL, stmt->sourcetype);
    targettypeid = typenameTypeId(NULL, stmt->targettype);
    sourcetyptype = get_typtype(sourcetypeid);
    targettyptype = get_typtype(targettypeid);

    // Validate types - no pseudo-types allowed
    if (sourcetyptype == TYPTYPE_PSEUDO || targettyptype == TYPTYPE_PSEUDO) {
        ereport(ERROR, "pseudo-types not allowed in casts");
    }

    // Check permissions - must own or have usage on both types
    if (!object_ownercheck(TypeRelationId, sourcetypeid, GetUserId()) &&
        !object_ownercheck(TypeRelationId, targettypeid, GetUserId())) {
        ereport(ERROR, "insufficient privileges");
    }

    // Warn about domain types (allowed but discouraged)
    if (sourcetyptype == TYPTYPE_DOMAIN || targettyptype == TYPTYPE_DOMAIN) {
        ereport(WARNING, "cast with domain types may be ignored");
    }

    // Determine cast method
    if (stmt->func != NULL)
        castmethod = COERCION_METHOD_FUNCTION;
    else if (stmt->inout)
        castmethod = COERCION_METHOD_INOUT;
    else
        castmethod = COERCION_METHOD_BINARY;

    // For function-based casts, validate the function
    if (castmethod == COERCION_METHOD_FUNCTION) {
        funcid = LookupFuncWithArgs(OBJECT_FUNCTION, stmt->func, false);

        // Validate function signature (1-3 args, proper types)
        HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
        Form_pg_proc procstruct = (Form_pg_proc) GETSTRUCT(tuple);

        if (procstruct->pronargs < 1 || procstruct->pronargs > 3) {
            ereport(ERROR, "cast function must take 1-3 arguments");
        }

        // Check argument and return type compatibility
        if (!IsBinaryCoercibleWithCast(sourcetypeid, procstruct->proargtypes.values[0], NULL)) {
            ereport(ERROR, "function argument must match source type");
        }

        ReleaseSysCache(tuple);
    }

    // For binary casts, require superuser and check physical compatibility
    if (castmethod == COERCION_METHOD_BINARY) {
        if (!superuser()) {
            ereport(ERROR, "must be superuser for binary casts");
        }

        // Check type size, alignment, and pass-by-value compatibility
        int16 typ1len, typ2len;
        bool typ1byval, typ2byval;
        char typ1align, typ2align;

        get_typlenbyvalalign(sourcetypeid, &typ1len, &typ1byval, &typ1align);
        get_typlenbyvalalign(targettypeid, &typ2len, &typ2byval, &typ2align);

        if (typ1len != typ2len || typ1byval != typ2byval || typ1align != typ2align) {
            ereport(ERROR, "types not physically compatible");
        }

        // Reject certain type categories for binary casts
        if (sourcetyptype == TYPTYPE_COMPOSITE || targettyptype == TYPTYPE_COMPOSITE ||
            sourcetyptype == TYPTYPE_RANGE || targettyptype == TYPTYPE_RANGE ||
            sourcetyptype == TYPTYPE_ENUM || targettyptype == TYPTYPE_ENUM) {
            ereport(ERROR, "type category not allowed for binary casts");
        }
    }

    // Convert coercion context to internal code
    switch (stmt->context) {
        case COERCION_IMPLICIT:
            castcontext = COERCION_CODE_IMPLICIT;
            break;
        case COERCION_ASSIGNMENT:
            castcontext = COERCION_CODE_ASSIGNMENT;
            break;
        case COERCION_EXPLICIT:
            castcontext = COERCION_CODE_EXPLICIT;
            break;
    }

    // Create the cast entry in system catalogs
    myself = CastCreate(sourcetypeid, targettypeid, funcid, InvalidOid, InvalidOid,
                        castcontext, castmethod, DEPENDENCY_NORMAL);

    return myself;
}
```