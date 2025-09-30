# FuncnameGetCandidates

## Location
[src/backend/catalog/namespace.c:1192-1584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L1192-L1584)

## Overview
Retrieves a list of function candidates that match a given function name and argument criteria, supporting various call conventions and argument matching strategies.

## Definition

```c
struct the name list */
	DeconstructQualifiedName(names, &schemaname, &funcname);
```
## Detailed Description
FuncnameGetCandidates is a comprehensive function lookup mechanism that finds all PostgreSQL functions matching specified criteria. It supports both qualified and unqualified function names, handles positional and named argument calls, and can expand variadic functions and default arguments. The function performs namespace-aware searches, returning candidates from either a specific schema or all schemas in the search path.

The function handles complex argument matching scenarios including variadic functions, default parameter expansion, named/mixed notation calls, and OUT parameter considerations. It implements sophisticated conflict resolution to handle cases where multiple functions could match the same call signature, preferring non-variadic over variadic functions and earlier namespace entries over later ones.

## Parameters / Member Variables
- : List containing the possibly-qualified function name (schema.function or just function)
- : Number of arguments in the call (-1 means return all functions regardless of argument count)
- : List of argument names for named/mixed notation calls (NIL for positional calls)
- : Whether to consider variadic functions with matching or fewer arguments
- : Whether to consider functions that could match with default argument insertion
- : Whether OUT-mode arguments should be included in argument matching
- : Whether to return NULL instead of erroring for missing schemas or no matches

## Dependencies
- Functions called/Symbols referenced:
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - SearchSysCacheList1
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [MatchNamedCall](../M/MatchNamedCall.md)
  - ReleaseSysCacheList
- Called from (representative examples):
  - [func_get_detail](../f/func_get_detail.md)
  - [LookupFuncNameInternal](../L/LookupFuncNameInternal.md)
  - [regprocin](../r/regprocin.md)
  - [regprocout](../r/regprocout.md)
  - [regprocedurein](../r/regprocedurein.md)

## Notes and Other Information
- Returns FuncCandidateList which is a linked list of candidate functions
- Implements namespace masking where functions in earlier search path positions hide identically-named functions in later positions
- Handles ambiguous matches by returning a single entry with oid = 0 to represent conflicting candidates
- The returned candidate list guarantees no duplicate argument lists, but may contain multiple functions that expand to the same signature
- Supports PostgreSQL's complex function overloading and resolution rules
- Critical component of PostgreSQL's function resolution system used throughout the parser and type system

## Simplified Source

```c
FuncCandidateList
FuncnameGetCandidates(List *names, int nargs, List *argnames,
                      bool expand_variadic, bool expand_defaults,
                      bool include_out_arguments, bool missing_ok)
{
    FuncCandidateList resultList = NULL;
    bool any_special = false;
    char *schemaname;
    char *funcname;
    Oid namespaceId;
    CatCList *catlist;

    // Parse function name (schema.func or func)
    DeconstructQualifiedName(names, &schemaname, &funcname);

    // Determine search scope
    if (schemaname) {
        // Search specific schema
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if (!OidIsValid(namespaceId))
            return NULL;
    } else {
        // Search all schemas in path
        namespaceId = InvalidOid;
        recomputeNamespacePath();
    }

    // Get all functions with this name
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));

    // Examine each candidate function
    for (int i = 0; i < catlist->n_members; i++) {
        HeapTuple proctup = &catlist->members[i]->tuple;
        Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(proctup);

        // Check if function is in target namespace(s)
        if (!function_in_search_scope(procform, namespaceId))
            continue;

        // Get effective argument types and count
        Oid *proargtypes = get_function_arg_types(proctup, procform,
                                                 include_out_arguments);
        int pronargs = get_function_arg_count(procform, include_out_arguments);

        // Handle named/mixed notation calls
        int *argnumbers = NULL;
        if (argnames != NIL) {
            if (!match_named_arguments(proctup, nargs, argnames,
                                     include_out_arguments, pronargs, &argnumbers))
                continue;
            any_special = true;
        }

        // Check variadic and defaults expansion
        bool variadic = false;
        bool use_defaults = false;
        Oid va_elem_type = InvalidOid;

        if (!check_argument_compatibility(procform, nargs, argnames,
                                        expand_variadic, expand_defaults,
                                        &variadic, &use_defaults, &va_elem_type))
            continue;

        // Create candidate entry
        FuncCandidateList newResult = create_func_candidate(
            procform, proargtypes, pronargs, nargs, variadic, use_defaults,
            va_elem_type, argnumbers);

        // Handle conflicts with existing candidates
        if (resultList != NULL && (any_special || !OidIsValid(namespaceId))) {
            FuncCandidateList conflict = find_conflicting_candidate(resultList, newResult);
            if (conflict) {
                int preference = resolve_candidate_preference(newResult, conflict);
                if (preference > 0) {
                    pfree(newResult);
                    continue; // Keep old
                } else if (preference < 0) {
                    remove_candidate_from_list(&resultList, conflict);
                    // Add new below
                } else {
                    conflict->oid = InvalidOid; // Mark ambiguous
                    pfree(newResult);
                    continue;
                }
            }
        }

        // Add to result list
        newResult->next = resultList;
        resultList = newResult;
    }

    ReleaseSysCacheList(catlist);
    return resultList;
}
```