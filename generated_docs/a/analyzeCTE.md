# analyzeCTE

## Location
[src/backend/parser/parse_cte.c:243-570](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_cte.c#L243-L570)

## Overview
Performs the actual parse analysis transformation of one Common Table Expression (CTE), handling column type validation, SEARCH/CYCLE clause processing, and recursive CTE verification.

## Definition


## Detailed Description
This static function transforms a single CTE from its raw parsed form into an analyzed Query node. It handles several complex aspects of CTE processing:

1. **CYCLE clause preprocessing**: Determines data types for cycle mark values and validates operators before query analysis
2. **Query analysis**: Uses parse_sub_analyze to transform the CTE's query into its internal representation
3. **Type validation for recursive CTEs**: Ensures output column types and collations match between recursive and non-recursive terms
4. **SEARCH/CYCLE clause validation**: Verifies that SEARCH and CYCLE clauses are properly formed and reference valid columns
5. **Expandability checks**: Ensures recursive CTEs with SEARCH/CYCLE clauses meet SQL standard requirements

The function performs extensive error checking and provides detailed error messages for various invalid CTE constructs.

## Parameters / Member Variables
- : Parse state containing context information for error reporting and CTE namespace management
- : The CommonTableExpr node to be analyzed and transformed

## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](../t/transformExpr.md) - transforms cycle mark expressions
  - [select_common_type](../s/select_common_type.md) - determines common type for cycle mark values
  - [coerce_to_common_type](../c/coerce_to_common_type.md) - coerces expressions to common type
  - [parse_sub_analyze](../p/parse_sub_analyze.md) - performs main query analysis
  - [analyzeCTETargetList](analyzeCTETargetList.md) - analyzes CTE output column specifications
  - GetCTETargetList - retrieves target list from CTE
  - [lookup_type_cache](../l/lookup_type_cache.md) - looks up type operators for cycle detection
  - [get_negator](../g/get_negator.md) - finds inequality operator for cycle mark comparison
- Called from (representative examples):
  - [transformWithClause](../t/transformWithClause.md) - called for each CTE in recursive WITH processing
  - [transformWithClause](../t/transformWithClause.md) - called for each CTE in non-recursive WITH processing

## Notes and Other Information
- The function is static and only used within parse_cte.c
- Handles both recursive and non-recursive CTEs with different validation logic
- For recursive CTEs, validates that column types match between terms
- SEARCH and CYCLE clauses are only allowed on recursive CTEs
- Data-modifying CTEs are only allowed at the top level of queries
- All CTE queries are marked as canSetTag = false
- Provides comprehensive validation of SEARCH and CYCLE clause column references
- Ensures SQL standard "expandability" requirements for recursive CTEs with special clauses