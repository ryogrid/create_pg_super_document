# func_get_detail

## Location
[src/backend/parser/parse_func.c:1395-1740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_func.c#L1395-L1740)

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
  - [FuncnameGetCandidates](../F/FuncnameGetCandidates.md) (searches catalogs for function candidates)
  - [FuncNameAsType](../F/FuncNameAsType.md) (checks if function name is actually a type name)
  - [func_match_argtypes](func_match_argtypes.md) (filters type-compatible candidates)
  - [func_select_candidate](func_select_candidate.md) (resolves ambiguity among multiple candidates)
  - [find_coercion_pathway](find_coercion_pathway.md) (validates type coercion paths)
  - [SearchSysCache1](../S/SearchSysCache1.md) (retrieves function metadata from pg_proc)
- Called from (representative examples):
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md) (from parse_func.c:266)
  - [lookup_agg_function](../l/lookup_agg_function.md) (from pg_aggregate.c:848)
  - [generate_function_name](../g/generate_function_name.md) (from ruleutils.c:12993)

## Notes and Other Information
- Returns FuncDetailCode indicating the type of resolution (normal, aggregate, window, procedure, coercion, etc.)
- Handles both positional and named argument notation
- Supports PostgreSQL's sophisticated type coercion system
- Critical for distinguishing between functions, procedures, aggregates, and window functions
- Implements special handling for type coercion functions to avoid infinite recursion
- Processes default arguments correctly for both positional and named notation
- Essential component in PostgreSQL's polymorphic function resolution system

## Simplified Source

```c
FuncDetailCode func_get_detail(List *funcname, List *fargs, List *fargnames,
                               int nargs, Oid *argtypes,
                               bool expand_variadic, bool expand_defaults,
                               bool include_out_arguments,
                               Oid *funcid, Oid *rettype, bool *retset,
                               int *nvargs, Oid *vatype,
                               Oid **true_typeids, List **argdefaults) {
    FuncCandidateList raw_candidates;
    FuncCandidateList best_candidate;

    // Initialize output parameters
    *funcid = InvalidOid;
    *rettype = InvalidOid;
    *retset = false;
    // ... other initializations

    // Step 1: Get list of possible candidates from namespace search
    raw_candidates = FuncnameGetCandidates(funcname, nargs, fargnames,
                                         expand_variadic, expand_defaults,
                                         include_out_arguments, false);

    // Step 2: Look for exact match with argument types
    for (best_candidate = raw_candidates; best_candidate != NULL;
         best_candidate = best_candidate->next) {
        if (nargs == 0 ||
            memcmp(argtypes, best_candidate->args, nargs * sizeof(Oid)) == 0) {
            break; // Found exact match
        }
    }

    if (best_candidate == NULL) {
        // Step 3: Check if this is a type coercion (single argument function
        // where function name is actually a type name)
        if (nargs == 1 && fargs != NIL && fargnames == NIL) {
            Oid targetType = FuncNameAsType(funcname);
            if (OidIsValid(targetType)) {
                // Determine if valid coercion path exists
                if (is_valid_coercion(argtypes[0], targetType)) {
                    *rettype = targetType;
                    *true_typeids = argtypes;
                    return FUNCDETAIL_COERCION;
                }
            }
        }

        // Step 4: Try to match candidates with type conversion
        if (raw_candidates != NULL) {
            FuncCandidateList current_candidates;
            int ncandidates = func_match_argtypes(nargs, argtypes,
                                                raw_candidates, &current_candidates);

            if (ncandidates == 1) {
                best_candidate = current_candidates;
            } else if (ncandidates > 1) {
                // Multiple candidates - resolve ambiguity
                best_candidate = func_select_candidate(nargs, argtypes,
                                                     current_candidates);
                if (!best_candidate)
                    return FUNCDETAIL_MULTIPLE; // Ambiguous
            }
        }
    }

    if (best_candidate) {
        // Step 5: Extract function details from pg_proc catalog
        HeapTuple ftup = SearchSysCache1(PROCOID,
                                       ObjectIdGetDatum(best_candidate->oid));
        Form_pg_proc pform = (Form_pg_proc) GETSTRUCT(ftup);

        // Set output parameters
        *funcid = best_candidate->oid;
        *rettype = pform->prorettype;
        *retset = pform->proretset;
        *nvargs = best_candidate->nvargs;
        *vatype = pform->provariadic;
        *true_typeids = best_candidate->args;

        // Handle default arguments if requested
        if (argdefaults && best_candidate->ndargs > 0) {
            extract_default_arguments(ftup, best_candidate, argdefaults);
        }

        // Determine function type and return appropriate code
        FuncDetailCode result;
        switch (pform->prokind) {
            case PROKIND_AGGREGATE:  result = FUNCDETAIL_AGGREGATE; break;
            case PROKIND_FUNCTION:   result = FUNCDETAIL_NORMAL; break;
            case PROKIND_PROCEDURE:  result = FUNCDETAIL_PROCEDURE; break;
            case PROKIND_WINDOW:     result = FUNCDETAIL_WINDOWFUNC; break;
        }

        ReleaseSysCache(ftup);
        return result;
    }

    return FUNCDETAIL_NOTFOUND;
}
```