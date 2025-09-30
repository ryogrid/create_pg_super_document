# coerce_type

## Location
[src/backend/parser/parse_coerce.c:157-555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L157-L555)

## Overview
Converts an expression from one data type to a different type, implementing the core type coercion logic in PostgreSQL's parser system.

## Definition

```c
enum, range, or multirange
		 * type.  In particular the argument must *not* be an UNKNOWN
		 * constant.  If it is, we just fall through;
```
## Detailed Description
This function performs the fundamental type conversion operations in PostgreSQL, handling various coercion scenarios:

1. **Identity Cases**: Returns the node unchanged when no conversion is needed (same types) or when dealing with polymorphic pseudotypes
2. **Unknown Constants**: Converts UNKNOWN string literals by applying the target type's input function
3. **Parameter Coercion**: Delegates to custom parameter coercion hooks when available
4. **CollateExpr Handling**: Manages COLLATE clauses by pushing coercion underneath or discarding for non-collatable types
5. **Standard Coercion**: Uses the coercion pathway system to find and apply appropriate conversion functions
6. **Complex Type Coercion**: Handles RECORD and complex array type conversions
7. **Inheritance Coercion**: Manages subclass to superclass conversions using ConvertRowtypeExpr

The function assumes the caller has already verified the coercion is possible via . It focuses on type conversion only; typmod (length) constraints are typically handled separately by .

## Parameters / Member Variables
- : Parse state context (can be NULL if parameter type resolution is not needed)
- : Input expression tree to be converted
- : Current type OID of the input expression
- : Desired target type OID for conversion
- : Target typmod (usually -1, as length coercion is handled separately)
- : Coercion context indicating the circumstances of the conversion
- : Coercion format controlling display and behavior of the coercion
- : Parse location for error reporting, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - [find_coercion_pathway](../f/find_coercion_pathway.md)
  - [build_coercion_expression](../b/build_coercion_expression.md)
  - [coerce_to_domain](coerce_to_domain.md)
  - [coerce_record_to_complex](coerce_record_to_complex.md)
  - [getBaseType](../g/getBaseType.md)
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md)
  - [typeInheritsFrom](../t/typeInheritsFrom.md)
  - [stringTypeDatum](../s/stringTypeDatum.md)
  - [makeRelabelType](../m/makeRelabelType.md)
  - [type_is_collatable](../t/type_is_collatable.md)
- Called from (representative examples):
  - [coerce_to_target_type](coerce_to_target_type.md)
  - [transformArrayExpr](../t/transformArrayExpr.md)
  - [make_fn_arguments](../m/make_fn_arguments.md)
  - [coerce_to_common_type](coerce_to_common_type.md)
  - [buildMergedJoinVar](../b/buildMergedJoinVar.md)

## Notes and Other Information
- Must not modify the input expression tree; only adds decoration on top
- Special handling for INTERVAL type which requires typmod to be passed to input function
- Contains extensive logic for domain type handling, ensuring base type operations are performed first
- Includes debug code for detecting unstable input functions when RANDOMIZE_ALLOCATED_MEMORY is enabled
- Uses recursive calls for CollateExpr processing
- Implements inheritance-based coercion using ConvertRowtypeExpr for complex type conversions
- Located in src/backend/parser/parse_coerce.c:157-555

## Simplified Source

```c
Node *coerce_type(ParseState *pstate, Node *node,
                 Oid inputTypeId, Oid targetTypeId, int32 targetTypeMod,
                 CoercionContext ccontext, CoercionForm cformat, int location) {
    Node *result;
    CoercionPathType pathtype;
    Oid funcId;

    // No conversion needed if types are the same
    if (targetTypeId == inputTypeId || node == NULL) {
        return node;
    }

    // Handle polymorphic pseudotypes (ANY*, ANYELEMENT, etc.)
    if (is_polymorphic_pseudotype(targetTypeId)) {
        return node;  // Return as-is for polymorphic types
    }

    // Handle UNKNOWN string constants
    if (inputTypeId == UNKNOWNOID && IsA(node, Const)) {
        Const *con = (Const *) node;
        Const *newcon = makeNode(Const);
        Oid baseTypeId = getBaseType(targetTypeId);

        // Use target type's input function to convert the string
        Type baseType = typeidType(baseTypeId);
        newcon->consttype = baseTypeId;
        newcon->consttypmod = (baseTypeId == INTERVALOID) ? targetTypeMod : -1;
        newcon->constcollid = typeTypeCollation(baseType);
        newcon->constisnull = con->constisnull;

        if (!con->constisnull) {
            newcon->constvalue = stringTypeDatum(baseType,
                                                DatumGetCString(con->constvalue),
                                                newcon->consttypmod);
        }

        result = (Node *) newcon;

        // Apply domain constraints if needed
        if (baseTypeId != targetTypeId) {
            result = coerce_to_domain(result, baseTypeId, -1,
                                    targetTypeId, ccontext, cformat, location, false);
        }

        return result;
    }

    // Handle parameter coercion hooks
    if (IsA(node, Param) && pstate && pstate->p_coerce_param_hook) {
        result = pstate->p_coerce_param_hook(pstate, (Param *) node,
                                           targetTypeId, targetTypeMod, location);
        if (result) {
            return result;
        }
    }

    // Handle COLLATE expressions
    if (IsA(node, CollateExpr)) {
        CollateExpr *coll = (CollateExpr *) node;
        result = coerce_type(pstate, (Node *) coll->arg,
                           inputTypeId, targetTypeId, targetTypeMod,
                           ccontext, cformat, location);

        if (type_is_collatable(targetTypeId)) {
            CollateExpr *newcoll = makeNode(CollateExpr);
            newcoll->arg = (Expr *) result;
            newcoll->collOid = coll->collOid;
            newcoll->location = coll->location;
            result = (Node *) newcoll;
        }
        return result;
    }

    // Find coercion pathway
    pathtype = find_coercion_pathway(targetTypeId, inputTypeId, ccontext, &funcId);

    if (pathtype != COERCION_PATH_NONE) {
        if (pathtype != COERCION_PATH_RELABELTYPE) {
            // Apply conversion function
            Oid baseTypeId = getBaseType(targetTypeId);
            result = build_coercion_expression(node, pathtype, funcId,
                                             baseTypeId, targetTypeMod,
                                             ccontext, cformat, location);

            // Apply domain constraints if needed
            if (targetTypeId != baseTypeId) {
                result = coerce_to_domain(result, baseTypeId, targetTypeMod,
                                        targetTypeId, ccontext, cformat, location, true);
            }
        } else {
            // Just relabel the type
            result = coerce_to_domain(node, InvalidOid, -1, targetTypeId,
                                    ccontext, cformat, location, false);
            if (result == node) {
                RelabelType *r = makeRelabelType((Expr *) result,
                                               targetTypeId, -1, InvalidOid, cformat);
                r->location = location;
                result = (Node *) r;
            }
        }
        return result;
    }

    // Handle complex type conversions
    if (inputTypeId == RECORDOID && ISCOMPLEX(targetTypeId)) {
        return coerce_record_to_complex(pstate, node, targetTypeId,
                                      ccontext, cformat, location);
    }

    if (targetTypeId == RECORDOID && ISCOMPLEX(inputTypeId)) {
        return node;
    }

    // Handle inheritance-based coercion
    if (typeInheritsFrom(inputTypeId, targetTypeId) ||
        typeIsOfTypedTable(inputTypeId, targetTypeId)) {
        ConvertRowtypeExpr *r = makeNode(ConvertRowtypeExpr);
        r->arg = (Expr *) node;
        r->resulttype = targetTypeId;
        r->convertformat = cformat;
        r->location = location;
        return (Node *) r;
    }

    // Should not reach here if can_coerce_type was called first
    elog(ERROR, "failed to find conversion function from %s to %s",
         format_type_be(inputTypeId), format_type_be(targetTypeId));
    return NULL;
}
```