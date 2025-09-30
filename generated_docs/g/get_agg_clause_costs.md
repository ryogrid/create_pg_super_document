# get_agg_clause_costs

## Location
[src/backend/optimizer/prep/prepagg.c:560-696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepagg.c#L560-L696)

## Overview
Processes the PlannerInfo's aggregate transition and aggregate info lists to accumulate cost information for aggregate clauses, taking into account the expected partial-aggregation mode.

## Definition

```c
structure.  The aggregate definition can
			 * provide an estimate of the size.  If it doesn't, then we assume
			 * ALLOCSET_DEFAULT_INITSIZE, which is a good guess if the data is
			 * being kept in a private memory context, as is done by
			 * array_agg() for instance.
			 */
			if (transinfo->aggtransspace > 0)
				costs->transitionSpace += transinfo->aggtransspace;
```
## Detailed Description
This function is a critical component of PostgreSQL's query optimizer that calculates the cost estimates for aggregate operations. It processes two key lists from PlannerInfo: 'aggtransinfos' and 'agginfos', accumulating cost information based on the expected partial-aggregation mode specified by the aggsplit parameter.

The function performs several key operations:

1. **Transition Function Costs**: For each AggTransInfo, it adds the cost of aggregate transitions using either the transition function (transfn) or combine function (combinefn) depending on the aggregation split mode.

2. **Serialization/Deserialization Costs**: Accounts for costs of serializing and deserializing transition states when data needs to be passed between different aggregation phases.

3. **Input Expression and Filter Costs**: Calculates costs for evaluating input expressions and any aggregate filters, but only for initial aggregate nodes to avoid double-counting.

4. **Memory Space Estimation**: Estimates the total space needed for transition states, considering different data types and their storage requirements (pass-by-value vs pass-by-reference).

5. **Final Function Costs**: For each AggInfo, adds the cost of running the final function and processing direct arguments.

The costs are ADDED to the existing values in the costs structure, so the caller must initialize it to zero beforehand.

## Parameters / Member Variables
- : PlannerInfo structure containing the aggregate information lists and planner context
- : AggSplit enum value indicating the expected partial-aggregation mode, which affects cost estimates
- : AggClauseCosts structure where the calculated costs will be accumulated

## Dependencies
- Functions called/Symbols referenced:
  - [add_function_cost](../a/add_function_cost.md)
  - [cost_qual_eval_node](../c/cost_qual_eval_node.md)
  - [get_typavgwidth](get_typavgwidth.md)
  - linitial_node
  - DO_AGGSPLIT_COMBINE
  - DO_AGGSPLIT_DESERIALIZE
  - DO_AGGSPLIT_SERIALIZE
  - DO_AGGSPLIT_SKIPFINAL
  - ALLOCSET_SMALL_INITSIZE
  - ALLOCSET_DEFAULT_INITSIZE
- Called from (representative examples):
  - [create_grouping_paths](../c/create_grouping_paths.md) (src/backend/optimizer/plan/planner.c:3832)
  - [create_partial_grouping_paths](../c/create_partial_grouping_paths.md) (src/backend/optimizer/plan/planner.c:7363, 7367)

## Notes and Other Information
- The function handles special cases for different transition types, including INTERNAL types and array aggregates
- Memory space estimation is particularly sophisticated, accounting for different storage patterns like expanded arrays used by array_append()
- The function respects the aggregation split mode to avoid double-counting costs in multi-phase aggregation scenarios
- Cost calculations distinguish between startup costs (one-time) and per-tuple costs (repeated for each input row)
- The function is essential for the optimizer to make informed decisions about aggregation strategies (HashAgg vs GroupAgg, partial vs full aggregation)

## Simplified Source

```c
void
get_agg_clause_costs(PlannerInfo *root, AggSplit aggsplit, AggClauseCosts *costs)
{
    ListCell *lc;

    // Process aggregate transition information
    foreach(lc, root->aggtransinfos) {
        AggTransInfo *transinfo = lfirst_node(AggTransInfo, lc);

        // Add appropriate function execution costs
        if (DO_AGGSPLIT_COMBINE(aggsplit)) {
            // Cost for combining previously aggregated states
            add_function_cost(root, transinfo->combinefn_oid, NULL, &costs->transCost);
        } else {
            add_function_cost(root, transinfo->transfn_oid, NULL, &costs->transCost);
        }

        // Add serialization/deserialization costs if needed
        if (DO_AGGSPLIT_DESERIALIZE(aggsplit) && OidIsValid(transinfo->deserialfn_oid))
            add_function_cost(root, transinfo->deserialfn_oid, NULL, &costs->transCost);
        if (DO_AGGSPLIT_SERIALIZE(aggsplit) && OidIsValid(transinfo->serialfn_oid))
            add_function_cost(root, transinfo->serialfn_oid, NULL, &costs->finalCost);

        // Add input expression and filter costs (only for initial aggregate nodes)
        if (!DO_AGGSPLIT_COMBINE(aggsplit)) {
            QualCost argcosts;

            // Cost of input expressions
            cost_qual_eval_node(&argcosts, (Node *) transinfo->args, root);
            costs->transCost.startup += argcosts.startup;
            costs->transCost.per_tuple += argcosts.per_tuple;

            // Cost of aggregate filters
            if (transinfo->aggfilter) {
                cost_qual_eval_node(&argcosts, (Node *) transinfo->aggfilter, root);
                costs->transCost.startup += argcosts.startup;
                costs->transCost.per_tuple += argcosts.per_tuple;
            }
        }

        // Estimate memory space for transition states
        if (!transinfo->transtypeByVal) {
            // Pass-by-reference types need space estimation
            int32 avgwidth;

            if (transinfo->aggtransspace > 0)
                avgwidth = transinfo->aggtransspace;
            else if (transinfo->transfn_oid == F_ARRAY_APPEND)
                avgwidth = ALLOCSET_SMALL_INITSIZE;  // Expanded array estimate
            else
                avgwidth = get_typavgwidth(transinfo->aggtranstype, transinfo->aggtranstypmod);

            avgwidth = MAXALIGN(avgwidth);
            costs->transitionSpace += avgwidth + 2 * sizeof(void *);
        } else if (transinfo->aggtranstype == INTERNALOID) {
            // INTERNAL type: likely pointer to large data structure
            if (transinfo->aggtransspace > 0)
                costs->transitionSpace += transinfo->aggtransspace;
            else
                costs->transitionSpace += ALLOCSET_DEFAULT_INITSIZE;
        }
    }

    // Process final aggregate functions
    foreach(lc, root->agginfos) {
        AggInfo *agginfo = lfirst_node(AggInfo, lc);
        Aggref *aggref = linitial_node(Aggref, agginfo->aggrefs);

        // Add final function cost
        if (!DO_AGGSPLIT_SKIPFINAL(aggsplit) && OidIsValid(agginfo->finalfn_oid))
            add_function_cost(root, agginfo->finalfn_oid, NULL, &costs->finalCost);

        // Add direct arguments cost
        if (aggref->aggdirectargs) {
            QualCost argcosts;
            cost_qual_eval_node(&argcosts, (Node *) aggref->aggdirectargs, root);
            costs->finalCost.startup += argcosts.startup;
            costs->finalCost.per_tuple += argcosts.per_tuple;
        }
    }
}
```