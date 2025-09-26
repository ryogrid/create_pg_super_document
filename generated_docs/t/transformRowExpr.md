# transformRowExpr

## Location
[src/backend/parser/parse_expr.c:2176-2213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L2176-L2213)

## Overview
Transforms a ROW expression from parse tree format into executable format, creating anonymous column names and setting up the row structure for tuple formation.

## Definition
```c
static Node *
transformRowExpr(ParseState *pstate, RowExpr *r, bool allowDefault)
```

## Detailed Description
The `transformRowExpr` function converts parsed ROW expressions into their executable representation during semantic analysis. ROW expressions are used to construct composite values (records/tuples) from multiple individual expressions. The function performs several key operations:

1. **Expression Transformation**: Transforms all field expressions within the ROW using `transformExpressionList()`, respecting the current expression context and default value policies
2. **Column Limit Enforcement**: Validates that the number of columns does not exceed `MaxTupleAttributeNumber` (PostgreSQL\"s maximum tuple attribute limit)
3. **Type Assignment**: Sets the row type to `RECORDOID` (generic record type) with implicit cast coercion, allowing for later type refinement
4. **Column Name Generation**: Automatically generates anonymous field names in the format \"f1\", \"f2\", etc., since ROW expressions don\"t have explicit column names
5. **Location Preservation**: Maintains source location information for accurate error reporting

The function is essential for handling composite value construction in SQL, enabling operations like multi-column assignments and tuple comparisons.

## Parameters / Member Variables
- `pstate`: Parse state containing context information for the current parsing operation
- `r`: The raw ROW expression from the parse tree to be transformed
- `allowDefault`: Boolean flag indicating whether DEFAULT values are permitted in the field expressions

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new RowExpr node)
  - transformExpressionList (transforms list of field expressions)
  - list_length (gets number of elements in list)
  - makeString (creates string nodes for column names)
  - pstrdup (duplicates strings in appropriate memory context)
  - snprintf (formats column names)
  - ereport/ERROR (error reporting)
  - parser_errposition (reports error location)

- Called from (representative examples):
  - transformExprRecurse (main expression transformation dispatcher)
  - transformMultiAssignRef (handles multi-column assignment references)

## Notes and Other Information
- ROW expressions create anonymous composite types with generated field names (f1, f2, f3, etc.)
- The initial type is set to RECORDOID, but may be refined to a specific composite type during later analysis phases
- The `allowDefault` parameter controls whether DEFAULT keywords are accepted in field expressions
- Maximum tuple size is enforced at `MaxTupleAttributeNumber` columns to prevent system limits violations
- The `row_format` is set to `COERCE_IMPLICIT_CAST` to allow flexible type coercion during usage
- Column names are generated sequentially and stored as String nodes in the colnames list
- Location information is preserved from the original parse tree for debugging and error reporting