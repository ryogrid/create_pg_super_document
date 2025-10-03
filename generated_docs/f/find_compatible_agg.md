# find_compatible_agg

## Location
[src/backend/optimizer/prep/prepagg.c:380-457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepagg.c#L380-L457)

## Overview
Searches for a previously processed aggregate that is identical to the current one, enabling reuse of both transition state and final result for optimization.

## Definition
```c
static int find_compatible_agg(PlannerInfo *root, Aggref *newagg, List **same_input_transnos)
```

## Detailed Description
This function implements the first level of aggregate optimization by searching for completely identical aggregate function calls that can share both their transition state and final result. It performs a comprehensive comparison of aggregate properties to determine exact matches.

The function performs two main operations:

1. **Exact Match Search**: Looks for aggregates that are completely identical in all respects:
   - Same aggregate function OID (aggfnoid)
   - Same result type (aggtype) 
   - Same input/output collations (inputcollid, aggcollid)
   - Same arguments, direct arguments, ORDER BY, DISTINCT, and FILTER clauses
   - Same aggregate properties (star, variadic, kind, transition type)

2. **Compatible Transition Collection**: As a side effect, builds a list of existing aggregates that have the same input parameters but different functions, which could potentially share transition state (passed to find_compatible_trans if no exact match is found).

**Optimization Logic**:
- **Identical aggregates**: When found, returns the aggno immediately and clears same_input_transnos since the entire computation can be reused
- **Compatible inputs**: When aggregates have same inputs but different functions, adds their transno to same_input_transnos list if the aggregate is shareable
- **Volatile functions**: Immediately returns -1 if the aggregate contains volatile function calls, as these cannot be safely shared

**Validation Requirements**:
All of the following properties must match exactly for identical aggregates:
- Input collation, transition type, star flag, variadic flag, aggregate kind
- Arguments list, ORDER BY clause, DISTINCT clause, FILTER clause  
- Function OID, result type, result collation, direct arguments

This strict matching ensures that optimization is only applied when it's completely safe and will produce identical results.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing the agginfos list of previously processed aggregates
- `*newagg`: Aggref node representing the new aggregate being processed
- `**same_input_transnos`: Output parameter that receives a list of transition numbers for aggregates with compatible inputs but different functions
## Dependencies
- Functions called/Symbols referenced:
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [equal](../e/equal.md) (for comparing node trees)
  - lfirst_node, linitial_node (list access macros)
  - [list_free](../l/list_free.md), lappend_int (list manipulation)
- Called from (representative examples):
  - [preprocess_aggref](../p/preprocess_aggref.md)

## Notes and Other Information
- This is a static function only accessible within the same source file
- Returns -1 if no compatible aggregate is found, otherwise returns the aggno (index) of the matching aggregate
- The same_input_transnos list may contain duplicate transno values if multiple aggregates share the same transition state
- Volatile function check is performed first for performance, as it's a quick way to eliminate non-shareable aggregates
- The function assumes that agginfos list contains at least one Aggref in each AggInfo's aggrefs list (uses linitial_node)
- Builds the compatible transitions list incrementally while searching, avoiding a separate pass through agginfos
- The equal() function performs deep tree comparison of expression nodes to ensure structural identity

## Simplified Source

```c
static int
find_compatible_agg(PlannerInfo *root, Aggref *newagg,
                    List **same_input_transnos)
{
    ListCell *lc;
    int aggno;

    *same_input_transnos = NIL;

    // Can't reuse aggregates with volatile functions
    if (contain_volatile_functions((Node *) newagg))
        return -1;

    // Search through existing aggregates
    aggno = -1;
    foreach(lc, root->agginfos)
    {
        AggInfo *agginfo = lfirst_node(AggInfo, lc);
        Aggref *existingRef = linitial_node(Aggref, agginfo->aggrefs);

        aggno++;

        // Check if core properties match
        if (newagg->inputcollid != existingRef->inputcollid ||
            newagg->aggtranstype != existingRef->aggtranstype ||
            newagg->aggstar != existingRef->aggstar ||
            newagg->aggvariadic != existingRef->aggvariadic ||
            newagg->aggkind != existingRef->aggkind ||
            !equal(newagg->args, existingRef->args) ||
            !equal(newagg->aggorder, existingRef->aggorder) ||
            !equal(newagg->aggdistinct, existingRef->aggdistinct) ||
            !equal(newagg->aggfilter, existingRef->aggfilter))
            continue;

        // Check for exact function match
        if (newagg->aggfnoid == existingRef->aggfnoid &&
            newagg->aggtype == existingRef->aggtype &&
            newagg->aggcollid == existingRef->aggcollid &&
            equal(newagg->aggdirectargs, existingRef->aggdirectargs))
        {
            // Exact match found - can reuse entire result
            list_free(*same_input_transnos);
            *same_input_transnos = NIL;
            return aggno;
        }

        // Same inputs, different function - might share transition state
        if (agginfo->shareable)
            *same_input_transnos = lappend_int(*same_input_transnos,
                                              agginfo->transno);
    }

    return -1;  // No compatible aggregate found
}
```