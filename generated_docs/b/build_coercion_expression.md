# build_coercion_expression

## Location
[src/backend/parser/parse_coerce.c:839-1011](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L839-L1011)

## Overview
This function constructs an expression tree for applying a pg_cast entry, supporting both type coercion and length coercion operations.

## Definition

```c
struct;
```
## Detailed Description
The build_coercion_expression function is a central component of PostgreSQL's type coercion system. It builds appropriate expression nodes based on the specified coercion path type, creating different node structures depending on how the coercion should be performed.

The function handles three main coercion path types:
1. COERCION_PATH_FUNC: Creates a FuncExpr node that calls a specific coercion function
2. COERCION_PATH_ARRAYCOERCE: Creates an ArrayCoerceExpr node for array element-wise coercion
3. COERCION_PATH_COERCEVIAIO: Creates a CoerceViaIO node for text-based coercion

For function-based coercion, it validates the coercion function and constructs appropriate arguments including optional typmod and explicit coercion parameters. For array coercion, it recursively coerces individual elements using a CaseTestExpr placeholder.

## Parameters / Member Variables
- : The input expression node to be coerced
- : The type of coercion path to use (FUNC, ARRAYCOERCE, or COERCEVIAIO)
- : OID of the coercion function (valid only for COERCION_PATH_FUNC)
- : OID of the target data type
- : Type modifier for the target type
- : Coercion context indicating whether coercion is implicit, assignment, or explicit
- : Coercion format controlling display behavior
- : Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache (system catalog access)
  - [makeConst](../m/makeConst.md), makeFuncExpr, makeNode (node construction)
  - [exprType](../e/exprType.md), exprTypmod (expression type utilities)
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md), get_element_type (type utilities)
  - [coerce_to_target_type](../c/coerce_to_target_type.md) (recursive coercion)
  - list_make1, lappend (list operations)
- Called from:
  - [coerce_type](../c/coerce_type.md)
  - [coerce_type_typmod](../c/coerce_type_typmod.md)

## Notes and Other Information
- This is a static function, only accessible within parse_coerce.c
- Validates coercion functions to ensure they have proper signatures (1-3 arguments)
- For array coercion, uses CaseTestExpr as a placeholder for individual array elements
- The function assumes that domain types will be handled by coerce_to_domain in a higher-level call
- Supports passing typmod and explicit coercion flags to coercion functions when needed
- Error handling includes cache lookup failures and unsupported path types

## Simplified Source

```c
static Node *build_coercion_expression(Node *node, CoercionPathType pathtype,
                                       Oid funcId, Oid targetTypeId, int32 targetTypMod,
                                       CoercionContext ccontext, CoercionForm cformat,
                                       int location) {
    int nargs = 0;

    // Validate coercion function if provided
    if (OidIsValid(funcId)) {
        HeapTuple tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcId));
        if (!HeapTupleIsValid(tp))
            elog(ERROR, "cache lookup failed for function %u", funcId);

        Form_pg_proc procstruct = (Form_pg_proc) GETSTRUCT(tp);

        // Validate function signature for coercion use
        Assert(!procstruct->proretset);
        Assert(procstruct->prokind == PROKIND_FUNCTION);
        nargs = procstruct->pronargs;
        Assert(nargs >= 1 && nargs <= 3);

        ReleaseSysCache(tp);
    }

    // Handle different coercion path types
    switch (pathtype) {
        case COERCION_PATH_FUNC:
            // Build FuncExpr with coercion function
            return build_function_coercion(node, funcId, targetTypeId, targetTypMod,
                                         nargs, ccontext, cformat, location);

        case COERCION_PATH_ARRAYCOERCE:
            // Build ArrayCoerceExpr for element-wise array coercion
            return build_array_coercion(node, targetTypeId, targetTypMod,
                                       ccontext, cformat, location);

        case COERCION_PATH_COERCEVIAIO:
            // Build CoerceViaIO for text-based coercion
            return build_io_coercion(node, targetTypeId, cformat, location);

        default:
            elog(ERROR, "unsupported pathtype %d", (int) pathtype);
            return NULL;
    }
}

// Helper: Build function-based coercion
static Node *build_function_coercion(Node *node, Oid funcId, Oid targetTypeId,
                                    int32 targetTypMod, int nargs,
                                    CoercionContext ccontext, CoercionForm cformat,
                                    int location) {
    List *args = list_make1(node);

    // Add typmod parameter if function expects it
    if (nargs >= 2) {
        Const *typmod_const = makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
                                       Int32GetDatum(targetTypMod), false, true);
        args = lappend(args, typmod_const);
    }

    // Add explicit coercion flag if function expects it
    if (nargs == 3) {
        Const *explicit_const = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool),
                                        BoolGetDatum(ccontext == COERCION_EXPLICIT),
                                        false, true);
        args = lappend(args, explicit_const);
    }

    FuncExpr *fexpr = makeFuncExpr(funcId, targetTypeId, args,
                                  InvalidOid, InvalidOid, cformat);
    fexpr->location = location;
    return (Node *) fexpr;
}

// Helper: Build array element coercion
static Node *build_array_coercion(Node *node, Oid targetTypeId, int32 targetTypMod,
                                 CoercionContext ccontext, CoercionForm cformat,
                                 int location) {
    ArrayCoerceExpr *acoerce = makeNode(ArrayCoerceExpr);

    // Create placeholder for array elements
    CaseTestExpr *element_placeholder = makeNode(CaseTestExpr);
    Oid source_element_type = get_element_type(exprType(node));
    element_placeholder->typeId = source_element_type;
    element_placeholder->typeMod = exprTypmod(node);

    // Coerce individual array elements
    Oid target_element_type = get_element_type(targetTypeId);
    Node *elemexpr = coerce_to_target_type(NULL, (Node *) element_placeholder,
                                         source_element_type, target_element_type,
                                         targetTypMod, ccontext, cformat, location);

    acoerce->arg = (Expr *) node;
    acoerce->elemexpr = (Expr *) elemexpr;
    acoerce->resulttype = targetTypeId;
    acoerce->resulttypmod = exprTypmod(elemexpr);
    acoerce->coerceformat = cformat;
    acoerce->location = location;

    return (Node *) acoerce;
}

// Helper: Build I/O-based coercion
static Node *build_io_coercion(Node *node, Oid targetTypeId,
                              CoercionForm cformat, int location) {
    CoerceViaIO *iocoerce = makeNode(CoerceViaIO);

    iocoerce->arg = (Expr *) node;
    iocoerce->resulttype = targetTypeId;
    iocoerce->coerceformat = cformat;
    iocoerce->location = location;

    return (Node *) iocoerce;
}
```

**Simplification Notes:**
- Broke down the large function into smaller, focused helper functions
- Preserved the three main coercion path types and their essential logic
- Simplified validation and error handling while keeping critical checks
- Maintained the core algorithm: validate function, then build appropriate node type
- Reduced from ~170 lines to ~80 lines while preserving functionality
- Made the control flow clearer with explicit switch statement and helper functions