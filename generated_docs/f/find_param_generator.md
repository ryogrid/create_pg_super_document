# find_param_generator

## Location
[src/backend/utils/adt/ruleutils.c:8276-8372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L8276-L8372)

## Overview
Searches for a subplan or initplan that generates the value for a PARAM_EXEC parameter in PostgreSQL's query execution tree.

## Definition
static SubPlan *find_param_generator(Param *param, deparse_context *context, int *column_p)

## Detailed Description
This function attempts to locate the subplan or initplan that emits the value for a PARAM_EXEC parameter by traversing the query execution plan hierarchy. It searches through the current plan node and its ancestors to find a matching parameter generator. The function follows a systematic search pattern: first checking the innermost plan node's initplans, then examining MULTIEXPR_SUBLINK SubPlans in the plan's targetlist, and finally searching through ancestor nodes. When a match is found, it returns the generating SubPlan and sets the output column number.

## Parameters / Member Variables
- `param`: The Param node for which to find the generator (must be PARAM_EXEC type)
- `context`: Deparse context containing namespace information and plan hierarchy
- `column_p`: Output parameter to store the 0-based output column number of the generating subplan

## Dependencies
- Functions called/Symbols referenced:
  - [find_param_generator_initplan](find_param_generator_initplan.md)
  - foreach_node
  - foreach_int
  - foreach_current_index
  - deparse_namespace
  - [SubPlan](../S/SubPlan.md)
  - PARAM_EXEC
  - MULTIEXPR_SUBLINK
- Called from (representative examples):
  - [get_parameter](../g/get_parameter.md)

## Notes and Other Information
- Only processes PARAM_EXEC parameters; returns NULL for other parameter types
- Searches both initplans and MULTIEXPR_SUBLINK subplans in the targetlist
- Performs hierarchical search through ancestor nodes in the plan tree
- Returns NULL if no generator is found
- Sets *column_p to 0 initially to prevent compiler warnings
- Part of PostgreSQL's rule deparsing system for query plan visualization

## Simplified Source

```c
static SubPlan *find_param_generator(Param *param, deparse_context *context, int *column_p) {
    *column_p = 0;

    // Only handle PARAM_EXEC parameters
    if (param->paramkind != PARAM_EXEC)
        return NULL;

    deparse_namespace *dpns = (deparse_namespace *) linitial(context->namespaces);

    // Check innermost plan node's initplans first
    SubPlan *result = find_param_generator_initplan(param, dpns->plan, column_p);
    if (result)
        return result;

    // Check MULTIEXPR_SUBLINK SubPlans in targetlist
    foreach_node(TargetEntry, tle, dpns->plan->targetlist) {
        if (tle->expr && IsA(tle->expr, SubPlan)) {
            SubPlan *subplan = (SubPlan *) tle->expr;
            if (subplan->subLinkType == MULTIEXPR_SUBLINK) {
                foreach_int(paramid, subplan->setParam) {
                    if (paramid == param->paramid) {
                        *column_p = foreach_current_index(paramid);
                        return subplan;
                    }
                }
            }
        }
    }

    // Search through ancestor nodes
    foreach(lc, dpns->ancestors) {
        Node *ancestor = (Node *) lfirst(lc);

        if (IsA(ancestor, SubPlan)) {
            SubPlan *subplan = (SubPlan *) ancestor;
            foreach_int(paramid, subplan->paramIds) {
                if (paramid == param->paramid) {
                    *column_p = foreach_current_index(paramid);
                    return subplan;
                }
            }
            continue;
        }

        // Check Plan node's initplans
        result = find_param_generator_initplan(param, (Plan *) ancestor, column_p);
        if (result)
            return result;
    }

    return NULL;
}
```