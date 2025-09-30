# resolve_polymorphic_tupdesc

## Location
[src/backend/utils/fmgr/funcapi.c:744-1063](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/funcapi.c#L744-L1063)

## Overview
Resolves polymorphic column types in a function's result tuple descriptor by replacing them with concrete data types deduced from the function's input arguments.

## Definition
static bool resolve_polymorphic_tupdesc(TupleDesc tupdesc, oidvector *declared_args, Node *call_expr)

## Detailed Description
This function is the core polymorphic type resolution engine for function return types with OUT parameters. It processes a tuple descriptor containing polymorphic column types (ANYELEMENT, ANYARRAY, ANYRANGE, etc.) and replaces them with concrete types based on the actual types passed as input arguments.

The function handles both the traditional polymorphic type family (ANYELEMENT, ANYARRAY, ANYRANGE, ANYMULTIRANGE) and the newer ANYCOMPATIBLE family. It works in several phases:

1. **Detection Phase**: Scans the tuple descriptor to identify which polymorphic types are present in the output
2. **Extraction Phase**: Examines the input arguments to extract concrete types for each polymorphic type family
3. **Resolution Phase**: Uses the resolve_*_from_others() helper functions to deduce missing polymorphic types from known ones
4. **Collation Phase**: Determines appropriate collations for the resolved types
5. **Replacement Phase**: Updates the tuple descriptor with the concrete types and collations

The function supports type inference between related polymorphic types (e.g., deducing ANYARRAY from ANYELEMENT) and handles collation inheritance from input expressions.

## Parameters / Member Variables
- : The tuple descriptor containing polymorphic column types to be resolved
- : OID vector of the function's declared input argument types, indicating which are polymorphic
- : The function call expression containing actual argument types, or NULL if not available

## Dependencies
- Functions called/Symbols referenced:
  - [get_call_expr_argtype](../g/get_call_expr_argtype.md): Extracts actual argument type from call expression
  - [resolve_anyelement_from_others](resolve_anyelement_from_others.md): Resolves ANYELEMENT from other polymorphic types
  - [resolve_anyarray_from_others](resolve_anyarray_from_others.md): Resolves ANYARRAY from other polymorphic types  
  - [resolve_anyrange_from_others](resolve_anyrange_from_others.md): Resolves ANYRANGE from other polymorphic types
  - [resolve_anymultirange_from_others](resolve_anymultirange_from_others.md): Resolves ANYMULTIRANGE from other polymorphic types
  - [get_typcollation](../g/get_typcollation.md): Gets collation for a data type
  - [exprInputCollation](../e/exprInputCollation.md): Determines input collation from expression
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md): Initializes tuple descriptor entry with resolved type
  - [TupleDescInitEntryCollation](../T/TupleDescInitEntryCollation.md): Sets collation for tuple descriptor entry

- Called from (representative examples):
  - [internal_get_result_type](../i/internal_get_result_type.md): When determining result types for functions with OUT parameters

## Notes and Other Information
- This is a static function, only used within funcapi.c  
- Returns true if all polymorphic types could be resolved, false if insufficient information is available
- Handles both traditional polymorphic types (ANY*) and compatible polymorphic types (ANYCOMPATIBLE*)
- Collation handling differs between type families - [range](range.md) types don't use collations
- The function assumes the parser has already validated argument type consistency
- Located in src/backend/utils/fmgr/funcapi.c:744-1063

## Simplified Source

```c
static bool resolve_polymorphic_tupdesc(TupleDesc tupdesc, oidvector *declared_args, Node *call_expr) {
    int natts = tupdesc->natts;
    int nargs = declared_args->dim1;
    bool have_polymorphic_result = false;
    polymorphic_actuals poly_actuals, anyc_actuals;
    Oid anycollation = InvalidOid, anycompatcollation = InvalidOid;

    // Check if output has any polymorphic types
    for (int i = 0; i < natts; i++) {
        Oid typid = TupleDescAttr(tupdesc, i)->atttypid;
        if (IsPolymorphicType(typid)) {
            have_polymorphic_result = true;
            break;
        }
    }

    if (!have_polymorphic_result)
        return true;

    if (!call_expr)
        return false;  // No way to resolve types

    // Extract actual types from input arguments
    memset(&poly_actuals, 0, sizeof(poly_actuals));
    memset(&anyc_actuals, 0, sizeof(anyc_actuals));

    for (int i = 0; i < nargs; i++) {
        Oid declared_type = declared_args->values[i];
        Oid actual_type = get_call_expr_argtype(call_expr, i);

        if (!OidIsValid(actual_type))
            return false;

        // Store actual types for each polymorphic family
        switch (declared_type) {
            case ANYELEMENTOID:
            case ANYNONARRAYOID:
            case ANYENUMOID:
                if (!OidIsValid(poly_actuals.anyelement_type))
                    poly_actuals.anyelement_type = actual_type;
                break;

            case ANYARRAYOID:
                if (!OidIsValid(poly_actuals.anyarray_type))
                    poly_actuals.anyarray_type = actual_type;
                break;

            case ANYCOMPATIBLEOID:
            case ANYCOMPATIBLENONARRAYOID:
                if (!OidIsValid(anyc_actuals.anyelement_type))
                    anyc_actuals.anyelement_type = actual_type;
                break;

            // ... (similar for other polymorphic types)
        }
    }

    // Resolve missing types from known ones
    resolve_missing_polymorphic_types(&poly_actuals);
    resolve_missing_polymorphic_types(&anyc_actuals);

    // Determine collations
    if (OidIsValid(poly_actuals.anyelement_type))
        anycollation = get_typcollation(poly_actuals.anyelement_type);
    if (OidIsValid(anyc_actuals.anyelement_type))
        anycompatcollation = get_typcollation(anyc_actuals.anyelement_type);

    // Apply input collation if available
    Oid inputcollation = exprInputCollation(call_expr);
    if (OidIsValid(inputcollation)) {
        if (OidIsValid(anycollation))
            anycollation = inputcollation;
        if (OidIsValid(anycompatcollation))
            anycompatcollation = inputcollation;
    }

    // Replace polymorphic types in tuple descriptor
    for (int i = 0; i < natts; i++) {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);
        Oid resolved_type = InvalidOid;
        Oid collation = InvalidOid;

        switch (att->atttypid) {
            case ANYELEMENTOID:
            case ANYNONARRAYOID:
            case ANYENUMOID:
                resolved_type = poly_actuals.anyelement_type;
                collation = anycollation;
                break;

            case ANYARRAYOID:
                resolved_type = poly_actuals.anyarray_type;
                collation = anycollation;
                break;

            case ANYCOMPATIBLEOID:
            case ANYCOMPATIBLENONARRAYOID:
                resolved_type = anyc_actuals.anyelement_type;
                collation = anycompatcollation;
                break;

            // ... (similar for other polymorphic types)

            default:
                continue;  // Not a polymorphic type
        }

        if (OidIsValid(resolved_type)) {
            TupleDescInitEntry(tupdesc, i + 1, NameStr(att->attname), resolved_type, -1, 0);
            if (OidIsValid(collation))
                TupleDescInitEntryCollation(tupdesc, i + 1, collation);
        }
    }

    return true;
}
```