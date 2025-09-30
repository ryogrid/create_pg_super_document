# eval_const_expressions_mutator

## Location
[src/backend/optimizer/util/clauses.c:2440-3735](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L2440-L3735)

## Overview
This static function implements the core recursive logic for constant expression evaluation and simplification, serving as the workhorse behind both eval_const_expressions and estimate_expression_value functions.

## Definition

```c
struct
													 * equivalence */

					/*
					 * Code for op/func reduction is pretty bulky, so split it
					 * out as a separate function.
					 */
					simple = simplify_function(expr->opfuncid,
											   expr->opresulttype, -1,
											   expr->opcollid,
											   expr->inputcollid,
											   &args,
											   false,
											   false,
											   false,
											   context);
```
## Detailed Description
The  function is the recursive engine that performs comprehensive constant folding, expression simplification, and optimization across all PostgreSQL expression node types. It implements a tree-walking mutator pattern that examines each node type and applies appropriate simplifications while preserving expression semantics.

Key capabilities include:
- **Parameter substitution**: Replaces Param nodes with constant values when bound parameters are available and context permits
- **Function/operator evaluation**: Simplifies FuncExpr and OpExpr nodes by calling simplify_function for immutable operations
- **Boolean logic optimization**: Reduces BoolExpr (AND/OR/NOT) expressions using logical identities  
- **Type coercion simplification**: Optimizes RelabelType, CoerceViaIO, and other coercion expressions
- **CASE expression optimization**: Eliminates unreachable branches and constant conditions
- **Array/window function handling**: Processes complex expressions while expanding function arguments
- **Special expression types**: Handles DistinctExpr, NullTest, JsonValueExpr, and other specialized nodes

The function respects context settings to determine whether unsafe optimizations (estimate mode) are permitted and maintains proper recursion through expression trees.

## Parameters / Member Variables
- : Node pointer to the current expression node being processed and potentially optimized
- : eval_const_expressions_context structure containing bound parameters, estimation mode flags, active function list, and CASE context information

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md), nodeTag, copyObject
  - Parameter handling: ParamListInfo, ParamExternData, get_typlenbyval, datumCopy, makeConst
  - Function processing: expand_function_arguments, simplify_function, set_opfuncid
  - [Boolean](../B/Boolean.md) logic: simplify_or_arguments, simplify_and_arguments, negate_clause, make_andclause, make_orclause
  - Expression evaluation: ece_evaluate_expr, ece_function_is_safe, ece_all_arguments_const
  - Type handling: applyRelabelType, getTypeOutputInfo, getTypeInputInfo
  - Many specialized helper functions for different expression types
- Called from (representative examples):
  - [eval_const_expressions](eval_const_expressions.md) (clauses.c:2266)
  - [estimate_expression_value](estimate_expression_value.md) (clauses.c:2405)
  - (recursively calls itself throughout expression trees)

## Notes and Other Information
- Uses stack depth checking to prevent overflow during deep recursion
- Implements the expression tree mutator pattern, returning modified copies of nodes
- Central to PostgreSQL's query optimization pipeline, enabling significant performance improvements through constant folding
- Handles over 20 different PostgreSQL expression node types with specialized logic for each
- The function's behavior is controlled by context flags, allowing different optimization strategies for planning vs. execution
- Critical for eliminating redundant computation and enabling further optimizations in the query planner

## Simplified Source

```c
static Node *
eval_const_expressions_mutator(Node *node, eval_const_expressions_context *context)
{
    check_stack_depth();  // Prevent stack overflow in deep recursion

    if (node == NULL)
        return NULL;

    switch (nodeTag(node))
    {
        case T_Param:
            // Try to substitute parameter with constant value if available
            if (parameter_is_available_and_const(param, context))
                return create_const_from_param(param, context);
            return copyObject(node);

        case T_FuncExpr:
            // Simplify function calls - may evaluate to constants
            simple = simplify_function(expr->funcid, expr->args, context);
            if (simple)
                return simple;
            return create_new_funcexpr_with_simplified_args(expr, context);

        case T_OpExpr:
            // Simplify operators (like +, -, =, etc.)
            set_opfuncid(expr);  // Get underlying function
            simple = simplify_function(expr->opfuncid, expr->args, context);
            if (simple)
                return simple;

            // Special handling for boolean equality/inequality
            if (expr->opno == BooleanEqualOperator || expr->opno == BooleanNotEqualOperator)
                return simplify_boolean_equality(expr->opno, expr->args);

            return create_new_opexpr_with_simplified_args(expr, context);

        case T_BoolExpr:
            // Optimize AND/OR/NOT expressions
            switch (expr->boolop)
            {
                case OR_EXPR:
                    // Remove FALSE constants, short-circuit on TRUE
                    newargs = simplify_or_arguments(expr->args, context);
                    if (all_false) return makeBoolConst(false, false);
                    if (found_true) return makeBoolConst(true, false);
                    if (single_arg) return single_arg;
                    return make_orclause(newargs);

                case AND_EXPR:
                    // Remove TRUE constants, short-circuit on FALSE
                    newargs = simplify_and_arguments(expr->args, context);
                    if (any_false) return makeBoolConst(false, false);
                    if (all_true) return makeBoolConst(true, false);
                    if (single_arg) return single_arg;
                    return make_andclause(newargs);

                case NOT_EXPR:
                    // Use logical negation rules
                    arg = eval_const_expressions_mutator(expr->arg, context);
                    return negate_clause(arg);
            }

        case T_CaseExpr:
            // Optimize CASE expressions by eliminating unreachable branches
            newarg = eval_const_expressions_mutator(caseexpr->arg, context);

            // Process WHEN clauses, eliminating constant FALSE conditions
            foreach(when_clause, caseexpr->args)
            {
                condition = eval_const_expressions_mutator(when_clause->expr, context);

                if (is_constant_false(condition))
                    continue;  // Skip this branch

                if (is_constant_true(condition))
                {
                    // This branch always matches - use its result as default
                    defresult = eval_const_expressions_mutator(when_clause->result, context);
                    break;
                }

                // Keep this branch
                newargs = lappend(newargs, create_new_when_clause(condition, result));
            }

            if (no_branches_left)
                return defresult;

            return create_new_case_expr(newarg, newargs, defresult);

        case T_CoalesceExpr:
            // Optimize COALESCE by removing NULL constants
            foreach(arg, coalesceexpr->args)
            {
                e = eval_const_expressions_mutator(arg, context);

                if (is_null_const(e))
                    continue;  // Skip NULL arguments

                if (is_non_null_const(e))
                {
                    if (first_arg)
                        return e;  // First non-null constant is the result
                    newargs = lappend(newargs, e);
                    break;  // No need to check further args
                }

                newargs = lappend(newargs, e);
            }

            if (all_null)
                return makeNullConst(coalesceexpr->coalescetype);

            return create_new_coalesce(newargs);

        case T_NullTest:
            // Optimize IS NULL / IS NOT NULL tests
            arg = eval_const_expressions_mutator(ntest->arg, context);

            if (IsA(arg, Const))
            {
                // Can determine result immediately for constants
                bool result = (ntest->nulltesttype == IS_NULL) ?
                             ((Const *)arg)->constisnull :
                             !((Const *)arg)->constisnull;
                return makeBoolConst(result, false);
            }

            return create_new_nulltest(arg, ntest->nulltesttype);

        case T_RelabelType:
            // Simplify type relabeling - may be eliminable
            arg = eval_const_expressions_mutator(relabel->arg, context);
            return applyRelabelType(arg, relabel->resulttype, relabel->resulttypmod,
                                  relabel->resultcollid, relabel->relabelformat,
                                  relabel->location, true);

        // ... other expression types handled similarly ...

        default:
            // For unknown types, just recursively simplify subexpressions
            return ece_generic_processing(node);
    }
}
```