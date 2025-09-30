# can_minmax_aggs

## Location
[src/backend/optimizer/plan/planagg.c:236-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planagg.c#L236-L315)

## Overview
Examines all aggregates in a query to verify they are MIN/MAX aggregates and builds a list of MinMaxAggInfo nodes for optimization planning.

## Definition

```c
struct what is effectively a sub-SELECT query, so
	 * clone the current query level's state and adjust it to make it look
	 * like a subquery.  Any outer references will now be one level higher
	 * than before.  (This means that when we are done, there will be no Vars
	 * of level 1, which is why the subquery can become an initplan.)
	 */
	subroot = (PlannerInfo *) palloc(sizeof(PlannerInfo));
```
## Detailed Description
This function validates whether all aggregates in a query are eligible for MIN/MAX optimization by examining each aggregate through the following criteria:

1. **Aggregate Structure**: Must have exactly one argument (single-column aggregates)
2. **Order Independence**: Rejects aggregates with ORDER BY clauses, as these can affect results when operator classes recognize non-identical values as equal
3. **Filter Absence**: Currently rejects aggregates with FILTER clauses (future enhancement possibility)
4. **MIN/MAX Verification**: Uses  to confirm the aggregate has a sort operator (indicating it's MIN or MAX)
5. **Mutability Check**: Ensures the aggregate argument doesn't contain mutable functions that would prevent indexable access
6. **Type Validation**: Rejects row-type expressions due to complex IS NOT NULL semantics

For each valid aggregate, it creates a  node containing the aggregate's function OID, sort operator, target expression, and placeholder fields for later path planning.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and aggregate information
- : Output parameter - pointer to a list that will be populated with MinMaxAggInfo nodes for valid aggregates

## Dependencies
- Functions called/Symbols referenced:
  -  - Retrieves the sort operator for an aggregate function
  -  - Checks if expression contains non-stable functions
  -  - Determines if expression type is a row/composite type
  -  - Creates new MinMaxAggInfo nodes
- Called from (representative examples):
  -  (src/backend/optimizer/plan/planagg.c:143)

## Notes and Other Information
- Returns false if any aggregate is not eligible for MIN/MAX optimization, true if all are eligible
- Uses the AggInfo list created by  rather than scanning the query directly
- DISTINCT clauses in aggregates are ignored (don't affect optimization eligibility)
- Future enhancement could support FILTER clauses by adding them to generated subquery quals
- The ORDER BY restriction prevents optimization of aggregates where result order matters for equal values

## Simplified Source

```c
static bool can_minmax_aggs(PlannerInfo *root, List **context)
{
    ListCell *lc;

    // Iterate through all aggregates found during preprocessing
    foreach(lc, root->agginfos)
    {
        AggInfo *agginfo = lfirst_node(AggInfo, lc);
        Aggref *aggref = linitial_node(Aggref, agginfo->aggrefs);
        Oid aggsortop;
        TargetEntry *curTarget;
        MinMaxAggInfo *mminfo;

        // Basic validation: must have exactly one argument
        if (list_length(aggref->args) != 1)
            return false;  // Not MIN/MAX eligible

        // Reject aggregates with ORDER BY (affects result predictability)
        if (aggref->aggorder != NIL)
            return false;

        // Reject aggregates with FILTER clause (not yet supported)
        if (aggref->aggfilter != NULL)
            return false;

        // Verify this is actually a MIN/MAX aggregate by checking sort operator
        aggsortop = fetch_agg_sort_op(aggref->aggfnoid);
        if (!OidIsValid(aggsortop))
            return false;  // Not a MIN/MAX aggregate

        curTarget = (TargetEntry *) linitial(aggref->args);

        // Ensure expression is indexable (no mutable functions)
        if (contain_mutable_functions((Node *) curTarget->expr))
            return false;

        // Reject row types (complex semantics)
        if (type_is_rowtype(exprType((Node *) curTarget->expr)))
            return false;

        // Create MinMaxAggInfo node for this valid aggregate
        mminfo = makeNode(MinMaxAggInfo);
        mminfo->aggfnoid = aggref->aggfnoid;
        mminfo->aggsortop = aggsortop;
        mminfo->target = curTarget->expr;
        mminfo->subroot = NULL;  // Path planning happens later
        mminfo->path = NULL;
        mminfo->pathcost = 0;
        mminfo->param = NULL;

        *context = lappend(*context, mminfo);
    }

    return true;  // All aggregates are MIN/MAX eligible
}
```