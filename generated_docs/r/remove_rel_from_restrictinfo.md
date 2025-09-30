# remove_rel_from_restrictinfo

## Location
[src/backend/optimizer/plan/analyzejoins.c:562-621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/analyzejoins.c#L562-L621)

## Overview
Removes references to specific relation IDs from a RestrictInfo structure's relid sets, handling both simple and complex OR clause scenarios.

## Definition

```c
static void
remove_rel_from_restrictinfo(RestrictInfo *rinfo, int relid, int ojrelid)
```
## Detailed Description
This function performs cleanup of RestrictInfo structures by removing references to eliminated relations from the clause_relids and required_relids bitmap sets. It's designed to handle the complexity of shared relid sets and nested OR/AND clause structures that can exist within RestrictInfo objects.

The function operates in several phases:

1. **Relid Set Cleanup**: Makes private copies of clause_relids and required_relids to avoid modifying shared structures, then removes both the base relation ID and outer join relation ID from these sets

2. **OR Clause Handling**: If the RestrictInfo contains an OR clause, recursively processes all sub-clauses to ensure complete cleanup throughout the clause tree

3. **AND Clause Processing**: For OR clauses containing AND sub-clauses, iterates through each AND argument and recursively applies the cleanup

The function is conservative about cleaning - it only touches clause_relids and required_relids, leaving nullingrel bits in contained Vars and PlaceHolderVars unchanged (though this may need improvement in future versions).

## Parameters / Member Variables
- : RestrictInfo structure containing the join restriction/qualification clause
- : Base relation ID to be removed from the RestrictInfo's relid sets  
- : Outer join relation ID to be removed from the RestrictInfo's relid sets

## Dependencies
- Functions called/Symbols referenced:
  - [bms_copy](../b/bms_copy.md): Creates private copies of bitmap sets to avoid modifying shared structures
  - [bms_del_member](../b/bms_del_member.md): Removes specific relation IDs from bitmap sets
  - [restriction_is_or_clause](restriction_is_or_clause.md): Checks if the RestrictInfo contains an OR clause
  - [is_orclause](../i/is_orclause.md), is_andclause: Node type checking functions for boolean expressions
  - [BoolExpr](../B/BoolExpr.md): Boolean expression node type for OR/AND clauses
  - [remove_rel_from_restrictinfo](remove_rel_from_restrictinfo.md): Recursive calls for processing sub-clauses

- Called from (representative examples):
  - [remove_rel_from_query](remove_rel_from_query.md): Main relation removal function during join elimination
  - [remove_rel_from_eclass](remove_rel_from_eclass.md): Equivalence class cleanup
  - Self-recursive calls for handling nested OR/AND structures

## Notes and Other Information
- The function creates private copies of relid sets because initsplan.c allows RestrictInfos to share relid sets with other structures
- Only processes clause_relids and required_relids - nullingrel bits in Vars and PHVs are left unchanged
- Handles complex nested boolean structures by recursively processing OR clauses containing AND sub-clauses
- Essential for maintaining consistency when relations are eliminated from the query plan
- The recursive nature ensures that all sub-clauses in complex boolean expressions are properly cleaned up
- Future improvements may be needed to handle nullingrel bits in contained expressions more comprehensively

## Simplified Source

```c
static void remove_rel_from_restrictinfo(RestrictInfo *rinfo, int relid, int ojrelid)
{
    // Make private copies of relid sets before modifying
    rinfo->clause_relids = bms_copy(rinfo->clause_relids);
    rinfo->clause_relids = bms_del_member(rinfo->clause_relids, relid);
    rinfo->clause_relids = bms_del_member(rinfo->clause_relids, ojrelid);

    rinfo->required_relids = bms_copy(rinfo->required_relids);
    rinfo->required_relids = bms_del_member(rinfo->required_relids, relid);
    rinfo->required_relids = bms_del_member(rinfo->required_relids, ojrelid);

    // Handle OR clauses by cleaning up sub-clauses recursively
    if (restriction_is_or_clause(rinfo)) {
        ListCell *lc;

        foreach(lc, ((BoolExpr *) rinfo->orclause)->args) {
            Node *orarg = (Node *) lfirst(lc);

            // Process AND sub-clauses
            if (is_andclause(orarg)) {
                List *andargs = ((BoolExpr *) orarg)->args;
                ListCell *lc2;

                foreach(lc2, andargs) {
                    RestrictInfo *rinfo2 = lfirst_node(RestrictInfo, lc2);
                    remove_rel_from_restrictinfo(rinfo2, relid, ojrelid);
                }
            }
            // Process simple RestrictInfo sub-clauses
            else {
                RestrictInfo *rinfo2 = castNode(RestrictInfo, orarg);
                remove_rel_from_restrictinfo(rinfo2, relid, ojrelid);
            }
        }
    }
}
```