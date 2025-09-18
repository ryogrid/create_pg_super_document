# finalize_grouping_exprs

## Location
src/backend/parser/parse_agg.c: 1483 - 1501

## Overview
Scans expression trees for GROUPING() function calls and validates/processes their arguments, handling join alias variable flattening for proper comparison.

## Definition


## Detailed Description
This function provides specialized processing for GROUPING() and related functions:

1. **Context initialization**: Sets up a check_ungrouped_columns_context structure with parameters appropriate for GROUPING function processing
2. **Walker delegation**: Invokes finalize_grouping_exprs_walker to perform the actual tree traversal and GROUPING function processing
3. **Separation of concerns**: Handles the in-place modification requirements that differ from the read-only validation performed by check_ungrouped_columns
4. **Join alias handling**: Works with original (unflattened) expression trees, performing individual argument flattening as needed for proper comparison

The function exists as a separate phase because GROUPING() function processing requires modifying nodes in-place, while ungrouped column checking may work with flattened copies of expressions.

## Parameters / Member Variables
- : Expression tree to scan for GROUPING() function calls
- : Parser state providing context and error reporting
- : Query structure containing grouping information
- : List of acceptable GROUP BY expressions for validation
- : Whether the query contains JOIN range table entries requiring alias flattening
- : Whether GROUP BY contains non-variable expressions

## Dependencies
- Functions called/Symbols referenced:
  - [finalize_grouping_exprs_walker](finalize_grouping_exprs_walker.md)
  - check_ungrouped_columns_context (struct)
- Called from (representative examples):
  - [parseCheckAggregates](../p/parseCheckAggregates.md) (twice - for target list and HAVING clause)

## Notes and Other Information
- Split from check_ungrouped_columns to handle different processing requirements
- Processes original unflattened expression trees, flattening individual GROUPING arguments as encountered
- Modifies nodes in-place rather than using the typical mutator pattern
- Part of PostgreSQL's GROUPING() function implementation for SQL standard compliance
- Works in coordination with check_ungrouped_columns but handles the specialized GROUPING function semantics
- The groupClauseCommonVars field is set to NIL since GROUPING function validation doesn't require functional dependency checking