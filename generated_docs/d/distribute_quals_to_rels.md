# distribute_quals_to_rels

## Location
[src/backend/optimizer/plan/initsplan.c:2119-2196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L2119-L2196)

## Overview
A convenience routine that applies distribute_qual_to_rels to each element of an AND'ed list of clauses during query planning.

## Definition

```c
static void
distribute_quals_to_rels(PlannerInfo *root, List *clauses,
						 JoinTreeItem *jtitem,
						 SpecialJoinInfo *sjinfo,
						 Index security_level,
						 Relids qualscope,
						 Relids ojscope,
						 Relids outerjoin_nonnullable,
						 Relids incompatible_relids,
						 bool allow_equivalence,
						 bool has_clone,
						 bool is_clone,
						 List **postponed_oj_qual_list)
```
## Detailed Description
This function serves as a wrapper that iterates through a list of qualification clauses and applies distribute_qual_to_rels to each individual clause. It's part of PostgreSQL's query planning phase, specifically handling the distribution of WHERE clause conditions and join conditions to appropriate relations in the query plan. The function maintains the same parameters as distribute_qual_to_rels but operates on a list of clauses rather than a single clause.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state
- : List of qualification clauses to be distributed
- : JoinTreeItem representing the current join tree context
- : SpecialJoinInfo for handling special join conditions
- : Index indicating the security level for row-level security
- : Relids representing the scope of qualification applicability
- : Relids representing outer join scope
- : Relids that are known to be non-nullable due to outer joins
- : Relids that are incompatible with the current context
- : Boolean flag controlling equivalence class creation
- : Boolean indicating if relation has clones
- : Boolean indicating if this is a clone relation
- : Output list for outer join qualifications that need later processing

## Dependencies
- Functions called/Symbols referenced:
  - [distribute_qual_to_rels](distribute_qual_to_rels.md)
  - [JoinTreeItem](../J/JoinTreeItem.md)
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md)
- Called from (representative examples):
  - [deconstruct_distribute](deconstruct_distribute.md)
  - [process_security_barrier_quals](../p/process_security_barrier_quals.md)
  - [deconstruct_distribute_oj_quals](deconstruct_distribute_oj_quals.md)

## Notes and Other Information
This function is a static helper function within the query planner's initialization phase. It simplifies the code by providing a batch processing interface for qualification distribution, eliminating the need for callers to manually iterate through clause lists. The function preserves all the complex parameter passing required by distribute_qual_to_rels, ensuring that each clause is processed with the same context and constraints.

## Simplified Source

```c
static void distribute_quals_to_rels(PlannerInfo *root, List *clauses,
                                    JoinTreeItem *jtitem,
                                    SpecialJoinInfo *sjinfo,
                                    Index security_level,
                                    Relids qualscope,
                                    Relids ojscope,
                                    Relids outerjoin_nonnullable,
                                    Relids incompatible_relids,
                                    bool allow_equivalence,
                                    bool has_clone,
                                    bool is_clone,
                                    List **postponed_oj_qual_list)
{
    ListCell *lc;

    // Process each clause in the AND'ed list
    foreach(lc, clauses) {
        Node *clause = (Node *) lfirst(lc);

        // Distribute this individual clause to appropriate relations
        distribute_qual_to_rels(root, clause,
                               jtitem,
                               sjinfo,
                               security_level,
                               qualscope,
                               ojscope,
                               outerjoin_nonnullable,
                               incompatible_relids,
                               allow_equivalence,
                               has_clone,
                               is_clone,
                               postponed_oj_qual_list);
    }
}
```