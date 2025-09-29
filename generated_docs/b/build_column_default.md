# build_column_default

## Location
[src/backend/rewrite/rewriteHandler.c:1218-1288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L1218-L1288)

## Overview
Creates an expression tree for the default value of a column, handling identity columns, column-specific defaults, and type defaults with proper type coercion.

## Definition
```c
Node *build_column_default(Relation rel, int attrno)
```

## Detailed Description
This function constructs a default value expression for a specific column in a relation. It follows a hierarchical approach to determine the appropriate default value:

1. **Identity columns**: For identity columns, creates a NextValueExpr that references the identity sequence
2. **Column-specific defaults**: If the column has an explicit default expression (`atthasdef`), retrieves it from the tuple descriptor
3. **Type defaults**: For non-generated columns without explicit defaults, falls back to the data type's default value
4. **Type coercion**: Ensures the final expression is properly coerced to the target column type

The function is critical for INSERT operations where values are not explicitly provided, as well as for ALTER TABLE operations that add new columns with defaults. It ensures type safety by coercing default expressions to match the target column's type and type modifier.

## Parameters / Member Variables
- `rel`: The relation (table) containing the column
- `attrno`: The attribute number (1-based) of the column for which to build the default

## Dependencies
- Functions called/Symbols referenced:
  - [NextValueExpr](../N/NextValueExpr.md)
  - [getIdentitySequence](../g/getIdentitySequence.md)
  - [TupleDescGetDefault](../T/TupleDescGetDefault.md)
  - [get_typdefault](../g/get_typdefault.md)
  - [coerce_to_target_type](../c/coerce_to_target_type.md)
  - COERCION_ASSIGNMENT
  - COERCE_IMPLICIT_CAST
- Called from (representative examples):
  - [BeginCopyFrom](../B/BeginCopyFrom.md)
  - [ATExecAddColumn](../A/ATExecAddColumn.md)
  - [ATExecSetExpression](../A/ATExecSetExpression.md)
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md)
  - [ExecInitStoredGenerated](../E/ExecInitStoredGenerated.md)
  - [slot_fill_defaults](../s/slot_fill_defaults.md)
  - [rewriteTargetListIU](../r/rewriteTargetListIU.md)
  - [rewriteValuesRTE](../r/rewriteValuesRTE.md)

## Notes and Other Information
- Returns NULL if no default value is available anywhere (column, type, or identity)
- Identity columns take precedence over other default mechanisms
- Generated columns are excluded from type default lookup
- Performs type coercion to ensure compatibility with target column type
- Errors are reported for type mismatches that cannot be resolved through coercion
- Used extensively in data modification operations and schema changes

## Simplified Source

```c
Node *
build_column_default(Relation rel, int attrno)
{
    TupleDesc rd_att = rel->rd_att;
    Form_pg_attribute att_tup = TupleDescAttr(rd_att, attrno - 1);
    Oid atttype = att_tup->atttypid;
    int32 atttypmod = att_tup->atttypmod;
    Node *expr = NULL;

    // Handle identity columns first
    if (att_tup->attidentity) {
        NextValueExpr *nve = makeNode(NextValueExpr);
        nve->seqid = getIdentitySequence(rel, attrno, false);
        nve->typeId = att_tup->atttypid;
        return (Node *) nve;
    }

    // Check for column-specific default
    if (att_tup->atthasdef) {
        expr = TupleDescGetDefault(rd_att, attrno);
        if (expr == NULL)
            elog(ERROR, "default expression not found for attribute %d", attrno);
    }

    // Fall back to type default for non-generated columns
    if (expr == NULL && !att_tup->attgenerated)
        expr = get_typdefault(atttype);

    // No default available
    if (expr == NULL)
        return NULL;

    // Coerce to target type if necessary
    Oid exprtype = exprType(expr);
    expr = coerce_to_target_type(NULL, expr, exprtype,
                                 atttype, atttypmod,
                                 COERCION_ASSIGNMENT,
                                 COERCE_IMPLICIT_CAST, -1);

    if (expr == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("column \"%s\" is of type %s but default expression is of type %s",
                        NameStr(att_tup->attname),
                        format_type_be(atttype),
                        format_type_be(exprtype))));

    return expr;
}
```