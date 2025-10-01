# transformJsonTableColumns

## Location
[src/backend/parser/parse_jsontable.c:251-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_jsontable.c#L251-L376)

## Overview
Creates a JsonTablePlan and transforms JSON_TABLE column specifications into their corresponding expression nodes and metadata for execution by the PostgreSQL engine.

## Definition

```c
static JsonTablePlan *
transformJsonTableColumns(JsonTableParseContext *cxt, List *columns,
						  List *passingArgs,
						  JsonTablePathSpec *pathspec)
```
## Detailed Description
This function is responsible for the core transformation of JSON_TABLE column specifications into executable expressions. It processes each column in the provided list and performs several critical operations:

1. **Column Type Processing**: Handles different column types (FOR ORDINALITY, REGULAR, FORMATTED, EXISTS, NESTED) with specific logic for each
2. **Type Inference and Conversion**: Determines appropriate PostgreSQL data types for each column, with automatic promotion from REGULAR to FORMATTED for complex types
3. **Expression Generation**: Creates JsonFuncExpr nodes for data-extracting columns and transforms them into executable expressions
4. **Metadata Collection**: Builds lists of column names, types, type modifiers, and collations for the TableFunc
5. **Ordinality Validation**: Ensures only one FOR ORDINALITY column exists per JSON_TABLE
6. **Nested Column Handling**: Recursively processes nested column structures and creates appropriate scan plans

The function integrates with the broader JSON_TABLE execution framework by creating JsonTablePathScan plans that can be executed during query runtime.

## Parameters / Member Variables
- : JsonTableParseContext containing parsing state, including the current JsonTable and TableFunc being processed
- : List of JsonTableColumn nodes representing the column specifications to transform
- : List of PASSING clause arguments that provide context values for JSON path expressions
- : JsonTablePathSpec defining the path specification for this level of columns

## Dependencies
- Functions called/Symbols referenced:
  - [typenameTypeIdAndMod](typenameTypeIdAndMod.md) (type resolution)
  - [transformJsonTableColumn](transformJsonTableColumn.md) (individual column transformation)
  - [transformExpr](transformExpr.md) (expression transformation)
  - [assign_expr_collations](../a/assign_expr_collations.md) (collation assignment)
  - [transformJsonTableNestedColumns](transformJsonTableNestedColumns.md) (recursive nested processing)
  - [makeJsonTablePathScan](../m/makeJsonTablePathScan.md) (scan plan creation)
  - [isCompositeType](../i/isCompositeType.md) (type checking)
  - [exprType](../e/exprType.md), exprTypmod, exprCollation (expression metadata)
- Called from (representative examples):
  - [transformJsonTable](transformJsonTable.md) (root level processing)
  - [transformJsonTableNestedColumns](transformJsonTableNestedColumns.md) (recursive nested processing)

## Notes and Other Information
- This is a static function, only accessible within the parse_jsontable.c module
- Automatically promotes REGULAR columns to FORMATTED when dealing with composite types or non-default wrapper/quotes behavior
- FOR ORDINALITY columns are assigned INT4OID type and receive special handling during execution
- The function maintains column ranges (colMin, colMax) to organize columns by their scan level
- Nested columns (JTC_NESTED) are skipped in the main loop and processed separately through transformJsonTableNestedColumns
- Error handling includes validation for multiple FOR ORDINALITY columns and unknown column types
- The resulting JsonTablePathScan integrates with PostgreSQL's execution planning system

## Simplified Source

```c
static JsonTablePlan *transformJsonTableColumns(JsonTableParseContext *cxt,
                                               List *columns, List *passingArgs,
                                               JsonTablePathSpec *pathspec) {
    ParseState *pstate = cxt->pstate;
    TableFunc *tf = cxt->tf;
    bool ordinality_found = false;
    bool errorOnError = cxt->jt->on_error && cxt->jt->on_error->btype == JSON_BEHAVIOR_ERROR;
    Oid contextItemTypid = exprType(tf->docexpr);
    int colMin = list_length(tf->colvalexprs);

    // Process each column specification
    foreach(col, columns) {
        JsonTableColumn *rawc = castNode(JsonTableColumn, lfirst(col));
        Oid typid;
        int32 typmod;
        Oid typcoll = InvalidOid;
        Node *colexpr;

        // Add column name for non-nested columns
        if (rawc->coltype != JTC_NESTED) {
            tf->colnames = lappend(tf->colnames, makeString(pstrdup(rawc->name)));
        }

        // Handle different column types
        switch (rawc->coltype) {
            case JTC_FOR_ORDINALITY:
                // Only one ordinality column allowed
                if (ordinality_found)
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                   errmsg("only one FOR ORDINALITY column is allowed")));
                ordinality_found = true;
                colexpr = NULL;
                typid = INT4OID;
                typmod = -1;
                break;

            case JTC_REGULAR:
                typenameTypeIdAndMod(pstate, rawc->typeName, &typid, &typmod);

                // Promote to FORMATTED for composite types or special options
                if (isCompositeType(typid) || rawc->quotes != JS_QUOTES_UNSPEC ||
                    rawc->wrapper != JSW_UNSPEC)
                    rawc->coltype = JTC_FORMATTED;
                // FALLTHROUGH

            case JTC_FORMATTED:
            case JTC_EXISTS:
                {
                    // Create JSON extraction expression
                    CaseTestExpr *param = makeNode(CaseTestExpr);
                    param->typeId = contextItemTypid;
                    param->typeMod = -1;

                    JsonFuncExpr *jfe = transformJsonTableColumn(rawc, (Node *) param,
                                                                passingArgs);
                    colexpr = transformExpr(pstate, (Node *) jfe, EXPR_KIND_FROM_FUNCTION);
                    assign_expr_collations(pstate, colexpr);

                    typid = exprType(colexpr);
                    typmod = exprTypmod(colexpr);
                    typcoll = exprCollation(colexpr);
                    break;
                }

            case JTC_NESTED:
                continue;  // Skip nested columns in main loop

            default:
                elog(ERROR, "unknown JSON_TABLE column type: %d", (int) rawc->coltype);
        }

        // Store column metadata in TableFunc
        tf->coltypes = lappend_oid(tf->coltypes, typid);
        tf->coltypmods = lappend_int(tf->coltypmods, typmod);
        tf->colcollations = lappend_oid(tf->colcollations, typcoll);
        tf->colvalexprs = lappend(tf->colvalexprs, colexpr);
    }

    // Determine column range for this scan level
    int colMax;
    if (list_length(tf->colvalexprs) == colMin) {
        colMax = colMin = -1;  // No columns besides nested ones
    } else {
        colMax = list_length(tf->colvalexprs) - 1;
    }

    // Process nested columns recursively
    JsonTablePlan *childplan = transformJsonTableNestedColumns(cxt, passingArgs, columns);

    // Create scan plan for this level
    return makeJsonTablePathScan(pathspec, errorOnError, colMin, colMax, childplan);
}
```