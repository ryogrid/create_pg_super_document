# initialize_aggregates

## Location
[src/backend/executor/nodeAgg.c:665-705](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L665-L705)

## Overview
Initializes all aggregate transition states for a new group of input values, supporting multiple grouping sets with selective reset capabilities.

## Definition
```c
static void initialize_aggregates(AggState *aggstate, AggStatePerGroup *pergroups, int numReset)
```

## Detailed Description
This function orchestrates the initialization of all aggregate functions for a new group, particularly in scenarios involving multiple grouping sets. It iterates through the specified number of grouping sets (or all sets if numReset is 0) and initializes each aggregate transition state within those sets. The function leverages the ordered nature of grouping sets where the most specific set (reset most frequently) comes first. For each grouping set, it calls select_current_set() to establish the proper context, then initializes each individual aggregate using initialize_aggregate(). This function is specifically designed for non-hash aggregates where grouping set context can be established upfront.

## Parameters / Member Variables
- `aggstate`: Pointer to AggState structure containing overall aggregate execution state
- `pergroups`: Array of per-group state structures, one for each grouping set
- `numReset`: Number of grouping sets to reset (0 means reset all sets)

## Dependencies
- Functions called/Symbols referenced:
  - [AggState](../A/AggState.md) (struct type)
  - [AggStatePerGroup](../A/AggStatePerGroup.md) (struct type)
  - [AggStatePerTrans](../A/AggStatePerTrans.md) (struct type)
  - [select_current_set](../s/select_current_set.md)
  - [initialize_aggregate](initialize_aggregate.md)
- Called from (representative examples):
  - [agg_retrieve_direct](../a/agg_retrieve_direct.md)

## Notes and Other Information
The function includes an important restriction: it cannot be used for hash aggregates because those require the grouping set number to be specified from higher-level calling code. The function uses Max() to handle cases where there might be no explicit grouping sets (defaulting to 1). The nested loop structure (grouping sets outer, transitions inner) ensures that all aggregates within each relevant grouping set are properly initialized. The function assumes CurrentMemoryContext is the per-query context when called, which is important for proper memory management during initialization.

## Simplified Source

```c
static void initialize_aggregates(AggState *aggstate,
                                  AggStatePerGroup *pergroups,
                                  int numReset) {
    int transno;
    int numGroupingSets = Max(aggstate->phase->numsets, 1);
    int setno = 0;
    int numTrans = aggstate->numtrans;
    AggStatePerTrans transstates = aggstate->pertrans;

    if (numReset == 0)
        numReset = numGroupingSets;

    // Initialize aggregates for each grouping set
    for (setno = 0; setno < numReset; setno++) {
        AggStatePerGroup pergroup = pergroups[setno];

        select_current_set(aggstate, setno, false);

        // Initialize each aggregate transition state
        for (transno = 0; transno < numTrans; transno++) {
            AggStatePerTrans pertrans = &transstates[transno];
            AggStatePerGroup pergroupstate = &pergroup[transno];

            initialize_aggregate(aggstate, pertrans, pergroupstate);
        }
    }
}
```