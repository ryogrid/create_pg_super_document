# simplify_boolean_equality

## Location
src/backend/optimizer/util/clauses.c: 3990 - 4058

## Overview
A specialized optimization function that simplifies boolean equality and inequality expressions involving constant boolean values, transforming patterns like "x = true" into "x" and "x = false" into "NOT x".

## Definition


## Detailed Description
This function serves as a subroutine for  to optimize boolean comparison operations when one operand is a constant boolean value. It implements logical simplification rules that are marginally useful in themselves but critical for ensuring expression equivalence recognition in contexts like partial index matching.

The function handles four main transformation patterns:
1. **Equality with TRUE**: "x = true" becomes "x", "true = x" becomes "x"
2. **Equality with FALSE**: "x = false" becomes "NOT x", "false = x" becomes "NOT x"
3. **Inequality with TRUE**: "x <> true" becomes "NOT x", "true <> x" becomes "NOT x"
4. **Inequality with FALSE**: "x <> false" becomes "x", "false <> x" becomes "x"

The function only operates when  has failed, meaning it will never see two constant inputs or a constant-NULL input. This ensures it focuses specifically on the boolean-constant simplification case.

## Parameters / Member Variables
- : The OID of the boolean operator being simplified (BooleanEqualOperator for "=" or inequality operator for "<>")
- : List containing exactly two arguments to the boolean operation (left and right operands)

## Dependencies
- Functions called/Symbols referenced:
  -  - Validates that exactly two arguments are provided
  -  - Extracts the left operand from the argument list
  -  - Extracts the right operand from the argument list
  -  - Type checking macro to identify Const nodes
  -  - Extracts boolean value from constant datum
  -  - Creates the logical negation of an expression
- Called from (representative examples):
  -  - Main constant expression evaluation function
  -  - Used in parallel query hazard assessment

## Notes and Other Information
- This is a static function, limiting its scope to the clauses.c file
- Returns NULL if no simplification is possible (when neither operand is a boolean constant)
- Assumes non-null constant values (asserted in the code)
- Critical for expression equivalence recognition in query optimization
- The transformations help normalize boolean expressions to canonical forms
- Particularly important for partial index matching where different but equivalent expressions need to be recognized as the same
- Only handles binary boolean operations (exactly 2 arguments expected)