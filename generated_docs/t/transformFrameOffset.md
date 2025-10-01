# transformFrameOffset

## Location
[src/backend/parser/parse_clause.c:3686-3822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L3686-L3822)

## Overview
Processes a window frame offset expression for SQL window functions, handling type coercion and validation for ROWS, RANGE, and GROUPS frame modes.

## Definition
```c
static Node *
transformFrameOffset(ParseState *pstate, int frameOptions,
                     Oid rangeopfamily, Oid rangeopcintype, Oid *inRangeFunc,
                     Node *clause)
```

## Detailed Description
This function transforms and validates offset expressions used in window frame specifications. It handles three different frame modes:

1. **ROWS mode**: Coerces the offset expression to `int8` (bigint), treating it as a row count
2. **RANGE mode**: Performs complex type resolution to find appropriate `in_range` support functions from the sort operator family, ensuring type compatibility between the offset expression and the ORDER BY column
3. **GROUPS mode**: Similar to ROWS, coerces to `int8` but operates on groups of equal values

For RANGE mode, the function searches the system catalog for `in_range` support functions that match the sort operator family and can accept the offset expression type. It handles multiple candidate functions by preferring exact type matches and providing detailed error messages when no suitable function is found.

The function also validates that the offset expression contains no variables, ensuring it evaluates to a constant value.

## Parameters / Member Variables
- `pstate`: Parser state containing context information for error reporting and expression transformation
- `frameOptions`: Bitmask indicating the frame mode (FRAMEOPTION_ROWS, FRAMEOPTION_RANGE, or FRAMEOPTION_GROUPS)
- `rangeopfamily`: OID of the sort operator family for the ORDER BY column (used in RANGE mode)
- `rangeopcintype`: Input data type that the sort operator is registered with (used in RANGE mode)  
- `inRangeFunc`: Output parameter that receives the OID of the selected in_range function (RANGE mode only)
- `clause`: The raw offset expression node to be transformed, can be NULL

## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](transformExpr.md)
  - [coerce_to_specific_type](../c/coerce_to_specific_type.md)
  - [exprType](../e/exprType.md)
  - SearchSysCacheList2
  - [can_coerce_type](../c/can_coerce_type.md)
  - [ReleaseCatCacheList](../R/ReleaseCatCacheList.md)
  - [checkExprIsVarFree](../c/checkExprIsVarFree.md)
  - [exprLocation](../e/exprLocation.md)
  - [format_type_be](../f/format_type_be.md)
  - ereport
- Called from (representative examples):
  - [transformWindowDefinitions](transformWindowDefinitions.md)

## Notes and Other Information
- Returns NULL if the clause parameter is NULL (no offset specified)
- For RANGE mode, the function implements sophisticated type resolution logic to handle cases where multiple in_range functions might be applicable
- Error messages provide specific guidance about type casting when automatic coercion fails
- The function enforces that frame offsets must be constant expressions (no variables allowed)
- Located in src/backend/parser/parse_clause.c:3686-3822

## Simplified Source

```c
static Node *transformFrameOffset(ParseState *pstate, int frameOptions,
                                  Oid rangeopfamily, Oid rangeopcintype, Oid *inRangeFunc,
                                  Node *clause) {
    const char *constructName = NULL;
    Node *node;

    *inRangeFunc = InvalidOid;

    // Quick exit if no offset expression
    if (clause == NULL)
        return NULL;

    if (frameOptions & FRAMEOPTION_ROWS) {
        // Transform and coerce to int8 for row count
        node = transformExpr(pstate, clause, EXPR_KIND_WINDOW_FRAME_ROWS);
        constructName = "ROWS";
        node = coerce_to_specific_type(pstate, node, INT8OID, constructName);

    } else if (frameOptions & FRAMEOPTION_RANGE) {
        // Transform expression and find compatible in_range function
        node = transformExpr(pstate, clause, EXPR_KIND_WINDOW_FRAME_RANGE);
        Oid nodeType = exprType(node);
        Oid preferredType = (nodeType != UNKNOWNOID) ? nodeType : rangeopcintype;

        // Search system catalog for compatible in_range functions
        CatCList *proclist = SearchSysCacheList2(AMPROCNUM,
                                                ObjectIdGetDatum(rangeopfamily),
                                                ObjectIdGetDatum(rangeopcintype));

        Oid selectedType = InvalidOid;
        Oid selectedFunc = InvalidOid;
        int nfuncs = 0, nmatches = 0;

        // Find best matching in_range function
        for (int i = 0; i < proclist->n_members; i++) {
            HeapTuple proctup = &proclist->members[i]->tuple;
            Form_pg_amproc procform = (Form_pg_amproc) GETSTRUCT(proctup);

            if (procform->amprocnum != BTINRANGE_PROC)
                continue;
            nfuncs++;

            if (!can_coerce_type(1, &nodeType, &procform->amprocrighttype, COERCION_IMPLICIT))
                continue;
            nmatches++;

            // Prefer exact type match
            if (selectedType != preferredType) {
                selectedType = procform->amprocrighttype;
                selectedFunc = procform->amproc;
            }
        }

        ReleaseCatCacheList(proclist);

        // Error handling for unsupported types
        if (nfuncs == 0 || nmatches == 0) {
            ereport(ERROR, /* appropriate error message */);
        }

        constructName = "RANGE";
        node = coerce_to_specific_type(pstate, node, selectedType, constructName);
        *inRangeFunc = selectedFunc;

    } else if (frameOptions & FRAMEOPTION_GROUPS) {
        // Transform and coerce to int8 for group count
        node = transformExpr(pstate, clause, EXPR_KIND_WINDOW_FRAME_GROUPS);
        constructName = "GROUPS";
        node = coerce_to_specific_type(pstate, node, INT8OID, constructName);
    }

    // Ensure offset expression contains no variables
    checkExprIsVarFree(pstate, node, constructName);

    return node;
}
```