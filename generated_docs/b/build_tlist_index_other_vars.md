# build_tlist_index_other_vars

## Location
[src/backend/optimizer/plan/setrefs.c:2739-2796](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L2739-L2796)

## Overview  
Builds a restricted targetlist index that only includes Vars belonging to relations other than a specified one, while allowing PlaceHolderVars to be matched.

## Definition

```c
structure with enough slots for all tlist entries */
	itlist = (indexed_tlist *)
		palloc(offsetof(indexed_tlist, vars) +
			   list_length(tlist) * sizeof(tlist_vinfo));
```
## Detailed Description
This function creates a specialized index structure similar to build_tlist_index, but with filtering applied to exclude variables from a specific relation. It is designed for scenarios where you need to match variables from all relations except one particular relation (specified by ignore_rel parameter).

The function builds an indexed_tlist structure containing only:
- Variables (Vars) that belong to relations other than ignore_rel
- PlaceHolderVars (has_ph_vars flag is set to true when found)

Notably, it does not set has_non_vars to true, meaning only Vars and PlaceHolderVars can be matched through this index - other expression types are not supported. This restriction makes the index more specialized but also more efficient for its intended use cases.

The primary use case is in contexts like RETURNING clauses where you need to reference variables from joined relations but want to exclude variables from the target relation being modified.

## Parameters / Member Variables
- : The targetlist (List of TargetEntry nodes) to be indexed
- : The relation number (varno) of variables to exclude from the index

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (with offsetof calculation)
  - [list_length](../l/list_length.md)  
  - lfirst (list iteration macro)
  - IsA (type checking macro)
  - offsetof (for structure size calculation)
- Called from (representative examples):
  - [set_returning_clause_references](../s/set_returning_clause_references.md) (src/backend/optimizer/plan/setrefs.c:3339)

## Notes and Other Information
- Similar to build_tlist_index but with relation-based filtering
- Explicitly excludes variables from the ignore_rel relation number
- Allows PlaceHolderVars to be indexed and matched
- Does not set has_non_vars flag, limiting matches to Vars and PlaceHolderVars only
- Uses the same variable-length structure allocation as build_tlist_index
- Preserves varnullingrels information for proper outer join null handling
- Designed for specialized use cases like RETURNING clause processing where target relation variables should be excluded
- The resulting structure can be freed with a single pfree() call

## Simplified Source

```c
static indexed_tlist *
build_tlist_index_other_vars(List *tlist, int ignore_rel)
{
    indexed_tlist *itlist;
    tlist_vinfo *vinfo;

    // Allocate index structure with space for all entries
    itlist = palloc(offsetof(indexed_tlist, vars) +
                   list_length(tlist) * sizeof(tlist_vinfo));

    // Initialize structure
    itlist->tlist = tlist;
    itlist->has_ph_vars = false;
    itlist->has_non_vars = false;

    // Process each target list entry
    vinfo = itlist->vars;
    foreach(l, tlist)
    {
        TargetEntry *tle = lfirst(l);

        // Index Vars from relations other than ignore_rel
        if (tle->expr && IsA(tle->expr, Var))
        {
            Var *var = (Var *) tle->expr;

            if (var->varno != ignore_rel)
            {
                // Store variable information in index
                vinfo->varno = var->varno;
                vinfo->varattno = var->varattno;
                vinfo->resno = tle->resno;
                vinfo->varnullingrels = var->varnullingrels;
                vinfo++;
            }
        }
        // Note presence of PlaceHolderVars
        else if (tle->expr && IsA(tle->expr, PlaceHolderVar))
            itlist->has_ph_vars = true;
    }

    itlist->num_vars = (vinfo - itlist->vars);
    return itlist;
}
```