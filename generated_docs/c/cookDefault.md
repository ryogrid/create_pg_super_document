# cookDefault

## Location
[src/backend/catalog/heap.c:2806-2882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L2806-L2882)

## Overview
Transforms raw default expressions into cooked format ready for storage, handling both regular column defaults and generated column expressions with appropriate validation and type coercion.

## Definition
Node *cookDefault(ParseState *pstate, Node *raw_default, Oid atttypid, int32 atttypmod, const char *attname, char attgenerated)

## Detailed Description
This function processes default value expressions for table columns, converting them from raw parse tree format into executable expressions suitable for storage in the system catalogs. It handles two distinct types of default expressions:

1. **Regular column defaults**: Standard default values that are evaluated once when a row is inserted
2. **Generated column expressions**: Computed values that are derived from other columns in the same row

The function performs comprehensive validation including type checking, mutability constraints for generated columns, and prohibition of column references in regular defaults. It also handles type coercion when the expression type does not match the target column type, providing detailed error messages for type mismatches.

## Parameters / Member Variables
- `pstate`: ParseState containing parser context and error reporting information
- `raw_default`: The raw Node representing the unparsed default expression
- `atttypid`: Target column data type OID (InvalidOid if no type coercion needed)
- `atttypmod`: Target column type modifier for precision/scale information
- `attname`: Column name used in error messages (only needed when type coercion is performed)
- `attgenerated`: Character indicating if this is a generated column (non-zero) or regular default (zero)

## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](../t/transformExpr.md): Converts parse tree to executable expression
  - [check_nested_generated](check_nested_generated.md): Validates generated column references
  - [contain_mutable_functions_after_planning](contain_mutable_functions_after_planning.md): Checks for non-immutable functions in generated columns
  - [contain_var_clause](contain_var_clause.md): Verifies absence of column references in regular defaults
  - [coerce_to_target_type](coerce_to_target_type.md): Handles type conversion and validation
  - [assign_expr_collations](../a/assign_expr_collations.md): Resolves collation assignments in the final expression
  - [exprType](../e/exprType.md): Determines the data type of an expression
  - [format_type_be](../f/format_type_be.md): Formats type names for error messages

- Called from (representative examples):
  - [AddRelationNewConstraints](../A/AddRelationNewConstraints.md): When adding constraints with default values
  - [DefineDomain](../D/DefineDomain.md): During domain definition with default values
  - [AlterDomainDefault](../A/AlterDomainDefault.md): When modifying domain default values

## Notes and Other Information
- The function enforces different validation rules for generated columns vs regular defaults
- Generated columns must use immutable functions only and cannot reference other generated columns
- Regular column defaults cannot contain column references at all
- Type coercion follows the same rules as assignment expressions in transformAssignedExpr()
- The function provides detailed error messages including type information and suggested solutions
- Collation assignment is handled as the final step to ensure proper collation semantics
- This function is central to PostgreSQL's default value and generated column implementation

## Simplified Source

```c
Node *
cookDefault(ParseState *pstate, Node *raw_default, Oid atttypid,
           int32 atttypmod, const char *attname, char attgenerated)
{
    Node *cooked_expr;

    Assert(raw_default != NULL);

    // Transform raw expression to executable form
    ExprKind expr_kind = attgenerated ? EXPR_KIND_GENERATED_COLUMN : EXPR_KIND_COLUMN_DEFAULT;
    cooked_expr = transformExpr(pstate, raw_default, expr_kind);

    if (attgenerated)
    {
        // Additional validation for generated columns
        check_nested_generated(pstate, cooked_expr);

        // Ensure generated expressions use only immutable functions
        if (contain_mutable_functions_after_planning((Expr *) cooked_expr))
            ereport(ERROR, "generation expression is not immutable");
    }
    else
    {
        // Regular defaults should not contain column references
        Assert(!contain_var_clause(cooked_expr));
    }

    // Coerce expression to target column type if specified
    if (OidIsValid(atttypid))
    {
        Oid source_type = exprType(cooked_expr);

        cooked_expr = coerce_to_target_type(pstate, cooked_expr, source_type,
                                           atttypid, atttypmod,
                                           COERCION_ASSIGNMENT,
                                           COERCE_IMPLICIT_CAST, -1);

        if (cooked_expr == NULL)
            ereport(ERROR,
                   "column \"%s\" is of type %s but default expression is of type %s",
                   attname, format_type_be(atttypid), format_type_be(source_type));
    }

    // Handle collation assignments
    assign_expr_collations(pstate, cooked_expr);

    return cooked_expr;
}
```