# analyzeCTETargetList

## Location
src/backend/parser/parse_cte.c: 571 - 647

## Overview
Computes derived fields of a CTE including column names, types, type modifiers, and collations from the transformed output target list.

## Definition


## Detailed Description
This function determines the final column specifications for a CTE based on its transformed target list. It handles several critical aspects:

1. **Column naming**: Uses alias column names when provided, falling back to target entry names for additional columns
2. **Type determination**: Extracts data types, type modifiers, and collations from target list expressions
3. **Unknown type resolution**: For recursive CTEs, converts UNKNOWN type columns to TEXT to ensure type consistency
4. **Validation**: Ensures that the number of available columns matches or exceeds the number of specified aliases

The function is called at different stages depending on CTE type:
- For non-recursive CTEs: Called after transforming the entire query
- For recursive CTEs: Called after transforming only the non-recursive term to establish baseline types

## Parameters / Member Variables
- : Parse state used primarily for error message context and location information
- : The CommonTableExpr node whose derived fields need to be computed
- : The transformed target list from which to derive column information

## Dependencies
- Functions called/Symbols referenced:
  - copyObject - creates copy of alias column names
  - [makeString](../m/makeString.md) - creates string nodes for column names
  - exprType - extracts data type from expressions
  - exprTypmod - extracts type modifier from expressions
  - [exprCollation](../e/exprCollation.md) - extracts collation from expressions
  - lappend_oid - appends OID values to lists
  - lappend_int - appends integer values to lists
- Called from (representative examples):
  - [analyzeCTE](analyzeCTE.md) - for non-recursive CTEs after query transformation
  - [determineRecursiveColTypes](../d/determineRecursiveColTypes.md) - for recursive CTEs after analyzing non-recursive term

## Notes and Other Information
- Fills in cte->ctecolnames, cte->ctecoltypes, cte->ctecoltypmods, and cte->ctecolcollations
- Allows alias lists to be shorter than the actual column count (PostgreSQL extension)
- For recursive CTEs, forces UNKNOWN columns to TEXT type with default collation
- Preserves existing collations even when converting UNKNOWN to TEXT
- Validates that enough columns are available to satisfy all specified aliases
- Skips junk entries in the target list during processing