# preprocess_groupclause

## Location
[src/backend/optimizer/plan/planner.c:2884-2979](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L2884-L2979)

## Overview
Reorders GROUP BY clause elements to match ORDER BY clause ordering, enabling optimization opportunities for combined sorting and grouping operations.

## Definition
```c
static List *preprocess_groupclause(PlannerInfo *root, List *force)
```

## Detailed Description
This function performs a crucial optimization by rearranging the GROUP BY clause to align with the ORDER BY clause when possible. Since GROUP BY ordering is semantically insignificant, this reordering can provide significant performance benefits:

**Key Optimization Goals:**
1. **Single Sort Operation**: When GROUP BY and ORDER BY have matching prefixes, a single sort can satisfy both requirements
2. **Incremental Sort Support**: Partial matches enable incremental sort optimizations  
3. **Index Optimization**: Reordering can match existing index sort orders

**Algorithm Logic:**

1. **Forced Ordering** (Grouping Sets):
   - When `force` parameter is provided, uses specified ordering for grouping sets
   - Retrieves matching SortGroupClause elements in forced order

2. **Normal Processing**:
   - Returns original order if no ORDER BY clause exists
   - Scans ORDER BY clauses to find matching GROUP BY elements
   - Builds new GROUP BY list maintaining ORDER BY prefix order
   - Stops prefix matching at first non-matching element

3. **Completion**:
   - Adds remaining GROUP BY items to preserve all grouping requirements
   - Validates that all GROUP BY elements have valid sort operators
   - Falls back to original order if any GROUP BY element is non-sortable

## Parameters
- `root`: PlannerInfo structure containing query parse information
- `force`: Optional list of sortgroupref indices to force specific ordering (used for grouping sets)

## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgroupref_clause](../g/get_sortgroupref_clause.md)
  - lfirst_int, list_copy, list_member_ptr
  - [equal](../e/equal.md) (node comparison)
  - OidIsValid
  - [SortGroupClause](../S/SortGroupClause.md) node handling
- Called from:
  - [grouping_planner](../g/grouping_planner.md)
  - standard_qp_extra  
  - [preprocess_grouping_sets](preprocess_grouping_sets.md)
  - [consider_groupingsets_paths](../c/consider_groupingsets_paths.md)

## Notes and Other Information
- **Return Value**: Fresh List containing reordered SortGroupClause elements (same objects as input)
- **Semantic Preservation**: GROUP BY ordering change does not affect query semantics
- **Incremental Sort**: Partial matches still provide optimization benefits through incremental sorting
- **Grouping Sets**: Special handling ensures proper ordering for complex grouping operations  
- **Index Utilization**: Enables better use of existing indexes that match the reordered GROUP BY
- **Parser Integration**: Unlike distinctClause, GROUP BY requires this processing since parser doesnt enforce ORDER BY matching
- Located in src/backend/optimizer/plan/planner.c:2884-2979

## Simplified Source

```c
static List *
preprocess_groupclause(PlannerInfo *root, List *force)
{
    Query      *parse = root->parse;
    List       *new_groupclause = NIL;

    // Handle grouping sets with forced ordering
    if (force) {
        foreach(sl, force) {
            Index ref = lfirst_int(sl);
            SortGroupClause *cl = get_sortgroupref_clause(ref, parse->groupClause);
            new_groupclause = lappend(new_groupclause, cl);
        }
        return new_groupclause;
    }

    // If no ORDER BY, keep original GROUP BY order
    if (parse->sortClause == NIL)
        return list_copy(parse->groupClause);

    // Build GROUP BY prefix that matches ORDER BY
    foreach(sl, parse->sortClause) {
        SortGroupClause *sc = lfirst_node(SortGroupClause, sl);

        // Look for matching GROUP BY clause
        foreach(gl, parse->groupClause) {
            SortGroupClause *gc = lfirst_node(SortGroupClause, gl);

            if (equal(gc, sc)) {
                new_groupclause = lappend(new_groupclause, gc);
                break;
            }
        }
        if (gl == NULL)
            break; // No match found, stop prefix matching
    }

    // If no matches at all, keep original order
    if (new_groupclause == NIL)
        return list_copy(parse->groupClause);

    // Add remaining GROUP BY items
    foreach(gl, parse->groupClause) {
        SortGroupClause *gc = lfirst_node(SortGroupClause, gl);

        // Skip if already included
        if (list_member_ptr(new_groupclause, gc))
            continue;

        // Check if sortable - give up if not
        if (!OidIsValid(gc->sortop))
            return list_copy(parse->groupClause);

        new_groupclause = lappend(new_groupclause, gc);
    }

    // Verify we have all original clauses
    Assert(list_length(parse->groupClause) == list_length(new_groupclause));
    return new_groupclause;
}
```