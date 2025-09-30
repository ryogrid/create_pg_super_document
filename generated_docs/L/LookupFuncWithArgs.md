# LookupFuncWithArgs

## Location
[src/backend/parser/parse_func.c:2206-2510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_func.c#L2206-L2510)

## Overview
LookupFuncWithArgs provides comprehensive function/procedure/aggregate lookup functionality using ObjectWithArgs structures, supporting both traditional input-only and modern input+output parameter matching for SQL standard compliance.

## Definition
```c
Oid LookupFuncWithArgs(ObjectType objtype, ObjectWithArgs *func, bool missing_ok)
```

## Detailed Description
This function implements the most sophisticated function lookup mechanism in PostgreSQL, supporting lookup of functions, procedures, aggregates, or any routine type. It handles ObjectWithArgs structures that can specify argument types with optional parameter modes. The function performs a two-stage lookup process: first using traditional PostgreSQL rules (input arguments only), then for procedures/routines, it attempts a second lookup including all parameters if no parameter modes are explicitly specified. This dual approach ensures SQL standard compliance for procedures while maintaining backward compatibility with traditional PostgreSQL function lookup semantics.

## Parameters / Member Variables
- `objtype`: Type of object to find (OBJECT_FUNCTION, OBJECT_PROCEDURE, OBJECT_AGGREGATE, or OBJECT_ROUTINE)
- `func`: ObjectWithArgs structure containing function name, argument types, and optionally full parameter specifications
- `missing_ok`: If true, return InvalidOid instead of throwing error when object not found

## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncNameInternal](LookupFuncNameInternal.md)  
  - [LookupTypeNameOid](LookupTypeNameOid.md)
  - [get_func_prokind](../g/get_func_prokind.md)
  - [list_length](../l/list_length.md)
  - lfirst_node
  - [func_signature_string](../f/func_signature_string.md)
  - [NameListToString](../N/NameListToString.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errhint](../e/errhint.md)
  - FUNC_MAX_ARGS
  - PROKIND_PROCEDURE
  - PROKIND_AGGREGATE
- Called from (representative examples):
  - DROP FUNCTION/PROCEDURE/AGGREGATE commands
  - ALTER FUNCTION/PROCEDURE commands
  - Various DDL statement parsers

## Notes and Other Information
The function implements complex logic to handle SQL standard procedure semantics where all parameters (IN, OUT, INOUT) are considered for matching, while preserving PostgreSQL's traditional function lookup that considers only input parameters. It validates that the found object matches the requested object type, with historical exceptions allowing functions to match aggregates and window functions. The two-stage lookup prevents ambiguity between functions and procedures with the same input signature but different output parameters. Maximum argument limits are enforced with appropriate error messages distinguishing between functions and procedures.

## Simplified Source

```c
Oid
LookupFuncWithArgs(ObjectType objtype, ObjectWithArgs *func, bool missing_ok) {
    Oid argoids[FUNC_MAX_ARGS];
    int argcount, nargs, i;
    ListCell *args_item;
    Oid oid;
    FuncLookupError lookupError;

    // Validate object type
    Assert(objtype == OBJECT_AGGREGATE || objtype == OBJECT_FUNCTION ||
           objtype == OBJECT_PROCEDURE || objtype == OBJECT_ROUTINE);

    // Check argument count limits
    argcount = list_length(func->objargs);
    if (argcount > FUNC_MAX_ARGS)
        ereport(ERROR, "too many arguments");

    // Convert argument type names to OIDs
    i = 0;
    foreach(args_item, func->objargs) {
        TypeName *t = lfirst_node(TypeName, args_item);
        argoids[i] = LookupTypeNameOid(NULL, t, missing_ok);
        if (!OidIsValid(argoids[i]))
            return InvalidOid; // missing_ok must be true
        i++;
    }

    // Set nargs for lookup (-1 means no args specified)
    nargs = func->args_unspecified ? -1 : argcount;

    // First lookup using traditional PostgreSQL rules (input args only)
    oid = LookupFuncNameInternal(func->args_unspecified ? objtype : OBJECT_ROUTINE,
                                 func->objname, nargs, argoids,
                                 false, missing_ok, &lookupError);

    // For procedures/routines, try second lookup with all parameters
    if ((objtype == OBJECT_PROCEDURE || objtype == OBJECT_ROUTINE) &&
        func->objfuncargs != NIL && lookupError != FUNCLOOKUP_AMBIGUOUS) {

        // Check if parameter modes are specified
        bool have_param_mode = false;
        foreach(args_item, func->objfuncargs) {
            FunctionParameter *fp = lfirst_node(FunctionParameter, args_item);
            if (fp->mode != FUNC_PARAM_DEFAULT) {
                have_param_mode = true;
                break;
            }
        }

        // If no parameter modes, try lookup with all parameters
        if (!have_param_mode) {
            Oid poid = LookupFuncNameInternal(objtype, func->objname,
                                              argcount, argoids,
                                              true, missing_ok, &lookupError);

            // Combine results, handle ambiguity
            if (OidIsValid(poid)) {
                if (OidIsValid(oid) && oid != poid) {
                    oid = InvalidOid;
                    lookupError = FUNCLOOKUP_AMBIGUOUS;
                } else {
                    oid = poid;
                }
            }
        }
    }

    if (OidIsValid(oid)) {
        // Validate object type matches what was found
        switch (objtype) {
            case OBJECT_FUNCTION:
                if (get_func_prokind(oid) == PROKIND_PROCEDURE)
                    ereport(ERROR, "is not a function");
                break;
            case OBJECT_PROCEDURE:
                if (get_func_prokind(oid) != PROKIND_PROCEDURE)
                    ereport(ERROR, "is not a procedure");
                break;
            case OBJECT_AGGREGATE:
                if (get_func_prokind(oid) != PROKIND_AGGREGATE)
                    ereport(ERROR, "is not an aggregate");
                break;
            default:
                // OBJECT_ROUTINE accepts anything
                break;
        }
        return oid;
    } else {
        // Handle lookup failures
        if (!missing_ok) {
            switch (lookupError) {
                case FUNCLOOKUP_NOSUCHFUNC:
                    ereport(ERROR, "function/procedure does not exist");
                    break;
                case FUNCLOOKUP_AMBIGUOUS:
                    ereport(ERROR, "function/procedure name is not unique");
                    break;
            }
        }
        return InvalidOid;
    }
}
```