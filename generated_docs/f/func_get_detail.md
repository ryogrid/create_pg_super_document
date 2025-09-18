# func_get_detail

## Location
src/backend/parser/parse_func.c: 1395 - 1740

## Overview
Main function resolution engine that searches system catalogs to find the best matching function for given name and arguments, handling exact matches, type coercion, and ambiguity resolution.

## Definition
```c
FuncDetailCode func_get_detail(List *funcname,
                               List *fargs,
                               List *fargnames,
                               int nargs,
                               Oid *argtypes,
                               bool expand_variadic,
                               bool expand_defaults,
                               bool include_out_arguments,
                               Oid *funcid,
                               Oid *rettype,
                               bool *retset,
                               int *nvargs,
                               Oid *vatype,
                               Oid **true_typeids,
                               List **argdefaults)
```

## Detailed Description
func_get_detail is the core function resolution function in PostgreSQL's parser system. It orchestrates the complete process of finding and validating function candidates from system catalogs. The function implements a multi-stage resolution process:

1. **Exact Match Search**: First attempts to find functions with exactly matching argument types
2. **Type Coercion Detection**: Recognizes single-argument calls that should be treated as type coercions
3. **Candidate Filtering**: Uses func_match_argtypes to filter type-compatible candidates
4. **Ambiguity Resolution**: Employs func_select_candidate to choose the best candidate when multiple matches exist
5. **Metadata Extraction**: Retrieves detailed function information from pg_proc catalog
6. **Default Argument Processing**: Handles function default arguments and named parameters

The function supports advanced PostgreSQL features including variadic functions, named arguments, default parameters, and OUT parameters. It returns detailed classification of the resolved function (normal, aggregate, window, procedure).

## Parameters / Member Variables
- `funcname`: List representing the function name (potentially schema-qualified)
- `fargs`: List of actual argument expressions for type coercion checking
- `fargnames`: List of argument names for named notation calls
- `nargs`: Number of arguments provided
- `argtypes`: Array of argument type OIDs
- `expand_variadic`: Whether to expand variadic functions
- `expand_defaults`: Whether to consider functions with default arguments
- `include_out_arguments`: Whether to include OUT parameters in resolution
- `funcid`: Return parameter for resolved function OID
- `rettype`: Return parameter for function return type
- `retset`: Return parameter indicating if function returns a set
- `nvargs`: Return parameter for number of variadic arguments
- `vatype`: Return parameter for variadic argument type
- `true_typeids`: Return parameter for actual function argument types
- `argdefaults`: Optional return parameter for default argument expressions

## Dependencies
- Functions called/Symbols referenced:
  - FuncnameGetCandidates (searches catalogs for function candidates)
  - FuncNameAsType (checks if function name is actually a type name)
  - func_match_argtypes (filters type-compatible candidates)
  - func_select_candidate (resolves ambiguity among multiple candidates)
  - find_coercion_pathway (validates type coercion paths)
  - SearchSysCache1 (retrieves function metadata from pg_proc)
- Called from (representative examples):
  - ParseFuncOrColumn (from parse_func.c:266)
  - lookup_agg_function (from pg_aggregate.c:848)
  - generate_function_name (from ruleutils.c:12993)

## Notes and Other Information
- Returns FuncDetailCode indicating the type of resolution (normal, aggregate, window, procedure, coercion, etc.)
- Handles both positional and named argument notation
- Supports PostgreSQL's sophisticated type coercion system
- Critical for distinguishing between functions, procedures, aggregates, and window functions
- Implements special handling for type coercion functions to avoid infinite recursion
- Processes default arguments correctly for both positional and named notation
- Essential component in PostgreSQL's polymorphic function resolution system