# ParseFuncOrColumn

## Location
src/backend/parser/parse_func.c: 90 - 922

## Overview
Parses a function call or column reference, handling both syntactic forms and resolving ambiguity between function calls and column projections in PostgreSQL's parser.

## Definition


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
  - func_get_detail (main function resolution)
  - ParseComplexProjection (for column projection handling)
  - transformWhereClause (for aggregate filter processing)
  - unify_hypothetical_args (for hypothetical aggregate validation)
  - enforce_generic_type_consistency (for polymorphic type handling)
  - make_fn_arguments (for argument type casting)
  - check_srf_call_placement (for set-returning function validation)
- Called from (representative examples):
  - transformFuncCall (from parse_expr.c:1474)
  - transformIndirection (from parse_expr.c:480)
  - transformCallStmt (from analyze.c:3112)

## Notes and Other Information
- The function supports both function syntax (fn != NULL) and column syntax (fn == NULL)
- For column syntax, returns NULL on failure rather than reporting errors
- Handles complex aggregate features like WITHIN GROUP, FILTER, and window functions
- Supports variadic functions with proper array construction for non-ANY variadics
- Enforces PostgreSQL's function argument limit (FUNC_MAX_ARGS)
- Special handling for procedures vs functions based on proc_call parameter
- Performs extensive validation for different function types and their allowed syntactic decorations