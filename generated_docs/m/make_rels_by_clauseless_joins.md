# make_rels_by_clauseless_joins

## Location
[src/backend/optimizer/path/joinrels.c:314-349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L314-L349)

## Overview
Creates Cartesian product join relations between a given relation and a list of other relations that don't share any common base relations, used as a fallback when no join clauses are available.

## Definition

```c
static void
make_rels_by_clauseless_joins(PlannerInfo *root,
							  RelOptInfo *old_rel,
							  List *other_rels)
```
## Detailed Description
The  function generates Cartesian product joins between a specified relation () and all compatible relations in a candidate list (). Unlike , this function does not require join clauses or join-order restrictions—it creates joins based purely on the absence of relation overlap.

This function serves as a fallback mechanism in PostgreSQL's join planning when no suitable clause-based joins can be formed. It is typically invoked in scenarios such as:
1. When a relation has no join clauses with other relations
2. As a last-ditch effort when the join search algorithm fails to find any clause-based joins at a particular level
3. In special cases involving sub-joinlists where relations only have join clauses with relations outside the current sub-problem

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing the query planning context
- `*old_rel`: The relation entry for the relation to be joined with others
- `*other_rels`: A list containing the other relations to be considered for Cartesian product joins
## Dependencies
- Functions called/Symbols referenced:
  - [bms_overlap](../b/bms_overlap.md)
  - [make_join_rel](make_join_rel.md)
- Called from (representative examples):
  - [join_search_one_level](../j/join_search_one_level.md) (used in two different contexts)

## Notes and Other Information
- Creates Cartesian product joins, which can be expensive but are sometimes necessary for query correctness
- Uses bitmapset overlap checking to ensure relations don't contain common base relations before joining
- Currently used primarily with initial relations but designed to work with join relations as well
- Results are automatically added to  through the  function
- Static function scope restricts direct usage to within the same source file
- Often used in conjunction with  in a preference hierarchy (clause-based joins preferred, Cartesian products as fallback)

## Simplified Source

```c
static void
make_rels_by_clauseless_joins(PlannerInfo *root,
                              RelOptInfo *old_rel,
                              List *other_rels)
{
    ListCell *l;

    // Iterate through each candidate relation
    foreach(l, other_rels)
    {
        RelOptInfo *other_rel = (RelOptInfo *) lfirst(l);

        // Only join if relations don't share any base relations
        if (!bms_overlap(other_rel->relids, old_rel->relids))
        {
            // Create a Cartesian product join
            (void) make_join_rel(root, old_rel, other_rel);
        }
    }
}
```