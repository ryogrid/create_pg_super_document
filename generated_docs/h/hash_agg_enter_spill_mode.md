# hash_agg_enter_spill_mode

## Location
[src/backend/executor/nodeAgg.c:1882-1916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L1882-L1916)

## Overview
Transitions hash aggregation into "spill mode" where new groups are no longer added to hash tables but are instead written to disk for later processing, enabling memory-bounded aggregation.

## Definition
```c
static void hash_agg_enter_spill_mode(AggState *aggstate)
```

## Detailed Description
This function orchestrates the transition from in-memory hash aggregation to disk-based spilling when memory limits are exceeded. It performs several critical operations:

1. **State Transition**: Sets the hash_spill_mode flag to true, preventing new group creation in hash tables
2. **Expression Recompilation**: Calls hashagg_recompile_expressions() to generate optimized expressions for spill mode that include null pointer checks
3. **Spill Infrastructure Setup**: On first spill, initializes the disk-based storage system including:
   - Creating a logical tape set for managing spilled data
   - Allocating spill structures for each hash table  
   - Initializing individual spill contexts with appropriate parameters

The function ensures that hash aggregation can continue processing even when memory constraints prevent storing all groups in memory simultaneously. Future tuples that would create new groups get spilled to disk instead of being processed immediately.

## Parameters / Member Variables
- `aggstate`: The aggregate state that will be transitioned to spill mode, containing hash tables and spill management structures

## Dependencies
- Functions called/Symbols referenced:
  - [AggState](../A/AggState.md)
  - [hashagg_recompile_expressions](hashagg_recompile_expressions.md)
  - [LogicalTapeSetCreate](../L/LogicalTapeSetCreate.md)
  - [HashAggSpill](../H/HashAggSpill.md)
  - [AggStatePerHash](../A/AggStatePerHash.md)
  - [hashagg_spill_init](hashagg_spill_init.md)
- Called from (representative examples):
  - [hash_agg_check_limits](hash_agg_check_limits.md)

## Notes and Other Information
- Once spill mode is activated, no new groups can be created in any hash table during the current phase
- The function includes assertion checks to ensure clean state during first-time spill initialization
- Spill structures are initialized for each hash table (setno) with appropriate parameters for group estimation and entry sizing
- This is a one-way transition - once entered, spill mode remains active for the current aggregation phase
- Critical for PostgreSQL's ability to handle large aggregation operations that exceed available memory

## Simplified Source

```c
static void
hash_agg_enter_spill_mode(AggState *aggstate)
{
    // Activate spill mode - no more new groups in hash tables
    aggstate->hash_spill_mode = true;
    hashagg_recompile_expressions(aggstate, aggstate->table_filled, true);

    // Initialize spill infrastructure on first use
    if (!aggstate->hash_ever_spilled)
    {
        aggstate->hash_ever_spilled = true;

        // Create tape set for disk storage
        aggstate->hash_tapeset = LogicalTapeSetCreate(true, NULL, -1);

        // Allocate spill structures for each hash table
        aggstate->hash_spills = palloc(sizeof(HashAggSpill) * aggstate->num_hashes);

        // Initialize each spill context
        for (int setno = 0; setno < aggstate->num_hashes; setno++)
        {
            AggStatePerHash perhash = &aggstate->perhash[setno];
            HashAggSpill *spill = &aggstate->hash_spills[setno];

            hashagg_spill_init(spill, aggstate->hash_tapeset, 0,
                             perhash->aggnode->numGroups,
                             aggstate->hashentrysize);
        }
    }
}
```

This simplified version shows the two-phase spill mode activation: first enabling spill mode and recompiling expressions, then initializing the disk-based spill infrastructure on first use.