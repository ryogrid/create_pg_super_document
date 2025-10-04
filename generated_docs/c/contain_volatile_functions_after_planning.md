# contain_volatile_functions_after_planning

## Location
[src/backend/optimizer/util/clauses.c:659-672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L659-L672)

## Overview
Tests whether a given expression contains volatile functions after performing expression planning. This function provides a safe interface for checking volatility from outside the planner context.

## Definition

```c
bool
contain_volatile_functions_after_planning(Expr *expr)
```
## Detailed Description
This function is a wrapper around  that ensures proper analysis by first running the expression through . The planning step is crucial for accurate volatility detection for two main reasons:

1. **Function default arguments insertion**: Default arguments are expanded during planning, which may affect volatility analysis. For example, a function with "default random()" would have its volatility characteristics properly identified only after defaults are inserted.

2. **Function inlining**: Inline-able functions are expanded during planning, potentially allowing more precise volatility analysis. Polymorphic functions are typically marked with the most volatile behavior across all possible input types, but after inlining for a specific input type, the actual volatility may be determined to be lower.

The function assumes that  does not modify its input expression, then proceeds to check for volatile functions in the planned expression.

## Parameters / Member Variables
- `*expr`: The expression to analyze for volatile function content
## Dependencies
- Functions called/Symbols referenced:
  - : Plans the expression before volatility analysis
  - : Performs the actual volatility check on the planned expression
- Called from (representative examples):
  -  (referenced in optimizer.h:144)

## Notes and Other Information
- This function is specifically designed to be safe for use outside the planner context
- The expression planning step is essential for accurate volatility detection
- The function returns a boolean indicating whether volatile functions are present
- Used in parallel processing contexts where volatility affects execution safety

## Simplified Source

```c
bool contain_volatile_functions_after_planning(Expr *expr) {
    // Run expression through planner to expand defaults and inline functions
    // This ensures accurate volatility detection by:
    // 1. Inserting function default arguments (e.g., "default random()")
    // 2. Inlining functions to get precise volatility for specific input types
    expr = expression_planner(expr);

    // Check the planned expression for volatile functions
    return contain_volatile_functions((Node *) expr);
}
```