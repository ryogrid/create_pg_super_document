# cookDefault

## Location
src/backend/catalog/heap.c: 2806 - 2882

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
  - transformExpr: Converts parse tree to executable expression
  - check_nested_generated: Validates generated column references
  - contain_mutable_functions_after_planning: Checks for non-immutable functions in generated columns
  - contain_var_clause: Verifies absence of column references in regular defaults
  - coerce_to_target_type: Handles type conversion and validation
  - assign_expr_collations: Resolves collation assignments in the final expression
  - exprType: Determines the data type of an expression
  - format_type_be: Formats type names for error messages

- Called from (representative examples):
  - AddRelationNewConstraints: When adding constraints with default values
  - DefineDomain: During domain definition with default values
  - AlterDomainDefault: When modifying domain default values

## Notes and Other Information
- The function enforces different validation rules for generated columns vs regular defaults
- Generated columns must use immutable functions only and cannot reference other generated columns
- Regular column defaults cannot contain column references at all
- Type coercion follows the same rules as assignment expressions in transformAssignedExpr()
- The function provides detailed error messages including type information and suggested solutions
- Collation assignment is handled as the final step to ensure proper collation semantics
- This function is central to PostgreSQL's default value and generated column implementation