# initialize_aggregates

## Location
src/backend/executor/nodeAgg.c: 665 - 705

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
  - AggState (struct type)
  - AggStatePerGroup (struct type)
  - AggStatePerTrans (struct type)
  - select_current_set
  - initialize_aggregate
- Called from (representative examples):
  - agg_retrieve_direct

## Notes and Other Information
The function includes an important restriction: it cannot be used for hash aggregates because those require the grouping set number to be specified from higher-level calling code. The function uses Max() to handle cases where there might be no explicit grouping sets (defaulting to 1). The nested loop structure (grouping sets outer, transitions inner) ensures that all aggregates within each relevant grouping set are properly initialized. The function assumes CurrentMemoryContext is the per-query context when called, which is important for proper memory management during initialization.