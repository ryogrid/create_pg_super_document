# ParseFuncOrColumn

## Location
[src/backend/parser/parse_func.c:90-922](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_func.c#L90-L922)

## Overview
Parses a function call or column reference, handling both syntactic forms and resolving ambiguity between function calls and column projections in PostgreSQL's parser.

## Definition

```c
structs.  Don't do this if dealing with column syntax,
	 * nor if we had WITHIN GROUP (because in that case it's critical to keep
	 * the argument count unchanged).
	 */
	nargs = 0;
```
## Detailed Description
ParseFuncOrColumn is a central function in PostgreSQL's parser that handles the ambiguity between function calls and column references. PostgreSQL treats notations like 'tab.col' and 'col(tab)' as equivalent when possible - a single-argument function call with a complex type argument can be interpreted as column projection if the function name matches an attribute of the type.

The function performs several key operations:
1. Extracts and validates argument type information
2. Handles named arguments and validates their usage
3. Determines whether the construct could be a column projection
4. Calls func_get_detail to resolve the function in system catalogs
5. Validates function types (normal, aggregate, window, procedure, coercion)
6. Handles special cases for ordered-set and hypothetical aggregates
7. Enforces type consistency and performs necessary type casting
8. Builds appropriate output structures (FuncExpr, Aggref, WindowFunc)

## Parameters / Member Variables
- : ParseState containing parsing context and state information
- : List of names representing the function name (potentially schema-qualified)
- : List of already-transformed argument expressions
- : Copy of pstate->p_last_srf from before transforming fargs for SRF placement checking
- : FuncCall struct containing function decoration (NULL for column syntax)
- : Boolean indicating if this is a CALL statement requiring procedure resolution
- : Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [func_get_detail](../f/func_get_detail.md) (main function resolution)
  - [ParseComplexProjection](ParseComplexProjection.md) (for column projection handling)
  - [transformWhereClause](../t/transformWhereClause.md) (for aggregate filter processing)
  - [unify_hypothetical_args](../u/unify_hypothetical_args.md) (for hypothetical aggregate validation)
  - [enforce_generic_type_consistency](../e/enforce_generic_type_consistency.md) (for polymorphic type handling)
  - [make_fn_arguments](../m/make_fn_arguments.md) (for argument type casting)
  - [check_srf_call_placement](../c/check_srf_call_placement.md) (for set-returning function validation)
- Called from (representative examples):
  - transformFuncCall (from parse_expr.c:1474)
  - [transformIndirection](../t/transformIndirection.md) (from parse_expr.c:480)
  - [transformCallStmt](../t/transformCallStmt.md) (from analyze.c:3112)

## Notes and Other Information
- The function supports both function syntax (fn != NULL) and column syntax (fn == NULL)
- For column syntax, returns NULL on failure rather than reporting errors
- Handles complex aggregate features like WITHIN GROUP, FILTER, and window functions
- Supports variadic functions with proper array construction for non-ANY variadics
- Enforces PostgreSQL's function argument limit (FUNC_MAX_ARGS)
- Special handling for procedures vs functions based on proc_call parameter
- Performs extensive validation for different function types and their allowed syntactic decorations