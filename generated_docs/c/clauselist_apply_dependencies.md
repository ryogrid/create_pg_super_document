# clauselist_apply_dependencies

## Location
[src/backend/statistics/dependencies.c:1014-1167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L1014-L1167)

## Overview
Applies functional dependencies to a list of clauses and returns the estimated selectivity by combining per-column selectivities using dependency degrees, accounting for the correlation between attributes.

## Definition

```c
static Selectivity
clauselist_apply_dependencies(PlannerInfo *root, List *clauses,
							  int varRelid, JoinType jointype,
							  SpecialJoinInfo *sjinfo,
							  MVDependency **dependencies, int ndependencies,
							  AttrNumber *list_attnums,
							  Bitmapset **estimatedclauses)
```
## Detailed Description
This function implements the core logic for applying functional dependencies during selectivity estimation. It processes clauses that are compatible with given dependencies and computes a more accurate combined selectivity than would result from assuming independence.

The algorithm works in several phases:

1. **Attribute Collection**: Extracts all attribute numbers (both implying and implied) from the given dependencies
2. **Individual Selectivity Computation**: Computes per-column selectivity estimates for each attribute using 
3. **Dependency Application**: Combines selectivities using the mathematical formula that accounts for dependency strength

The key mathematical formula used is:


Where  is the degree of dependency. For dependency chains (a->b->c), conditional probabilities are computed:


The function processes dependencies in reverse order to handle dependency chains correctly.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context
- : List of WHERE clauses to estimate selectivity for
- : Relation ID for the target relation
- : Type of join operation
- : Special join information structure
- : Array of functional dependencies to apply
- : Number of dependencies in the array
- : Array mapping clause positions to attribute numbers
- : Output bitmapset of clauses that were estimated

## Dependencies
- Functions called/Symbols referenced:
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_num_members](../b/bms_num_members.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_member_index](../b/bms_member_index.md)
  - [clauselist_selectivity_ext](clauselist_selectivity_ext.md)
  - CLAMP_PROBABILITY
  - [bms_free](../b/bms_free.md)
- Types used:
  - JoinType
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md)
  - MVDependency
- Called from (representative examples):
  - DependencyGenerator
  - [dependencies_clauselist_selectivity](../d/dependencies_clauselist_selectivity.md)

## Notes and Other Information
- Uses Min(P(a), P(b)) instead of P(a) for dependent rows to ensure the result doesn't exceed individual column selectivities
- Processes dependencies in reverse order to handle dependency chains correctly
- Marks all processed clauses in the estimatedclauses bitmapset to avoid double-counting
- The conditional probability formula ensures that dependency chains are handled properly
- Results are clamped using CLAMP_PROBABILITY to ensure valid probability ranges
- Memory management includes proper cleanup of allocated bitmapsets and arrays

## Simplified Source

```c
static Selectivity
clauselist_apply_dependencies(PlannerInfo *root, List *clauses, int varRelid,
                             JoinType jointype, SpecialJoinInfo *sjinfo,
                             MVDependency **dependencies, int ndependencies,
                             AttrNumber *list_attnums, Bitmapset **estimatedclauses)
{
    Bitmapset *attnums = NULL;
    int nattrs;
    Selectivity *attr_sel;

    // Collect all attribute numbers from dependencies
    for (int i = 0; i < ndependencies; i++) {
        for (int j = 0; j < dependencies[i]->nattributes; j++) {
            AttrNumber attnum = dependencies[i]->attributes[j];
            attnums = bms_add_member(attnums, attnum);
        }
    }

    // Compute per-column selectivities
    nattrs = bms_num_members(attnums);
    attr_sel = (Selectivity *) palloc(sizeof(Selectivity) * nattrs);

    int attidx = 0;
    int i = -1;
    while ((i = bms_next_member(attnums, i)) >= 0) {
        List *attr_clauses = NIL;

        // Collect clauses for this attribute
        int listidx = -1;
        foreach(lc, clauses) {
            listidx++;
            if (list_attnums[listidx] == i) {
                attr_clauses = lappend(attr_clauses, lfirst(lc));
                *estimatedclauses = bms_add_member(*estimatedclauses, listidx);
            }
        }

        // Compute selectivity for this attribute's clauses
        Selectivity simple_sel = clauselist_selectivity_ext(root, attr_clauses,
                                                           varRelid, jointype, sjinfo, false);
        attr_sel[attidx++] = simple_sel;
    }

    // Apply dependencies in reverse order to handle chains correctly
    for (int i = ndependencies - 1; i >= 0; i--) {
        MVDependency *dependency = dependencies[i];

        // Compute selectivity of implying attributes
        Selectivity s1 = 1.0;
        for (int j = 0; j < dependency->nattributes - 1; j++) {
            AttrNumber attnum = dependency->attributes[j];
            int attidx = bms_member_index(attnums, attnum);
            s1 *= attr_sel[attidx];
        }

        // Get selectivity of implied attribute
        AttrNumber implied_attnum = dependency->attributes[dependency->nattributes - 1];
        int implied_attidx = bms_member_index(attnums, implied_attnum);
        Selectivity s2 = attr_sel[implied_attidx];

        // Apply conditional probability formula: P(b|a) = f * Min(P(a),P(b))/P(a) + (1-f) * P(b)
        double f = dependency->degree;
        if (s1 <= s2)
            attr_sel[implied_attidx] = f + (1 - f) * s2;
        else
            attr_sel[implied_attidx] = f * s2 / s1 + (1 - f) * s2;
    }

    // Combine all selectivities
    Selectivity result = 1.0;
    for (int i = 0; i < nattrs; i++)
        result *= attr_sel[i];

    CLAMP_PROBABILITY(result);

    pfree(attr_sel);
    bms_free(attnums);

    return result;
}
```