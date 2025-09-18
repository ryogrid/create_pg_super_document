# AggStatePerAgg

## Location
src/include/nodes/execnodes.h: 2457 - 2457

## Overview
AggStatePerAgg is a typedef pointer to AggStatePerAggData structure that represents per-aggregate information in PostgreSQL's aggregation execution system, containing metadata and state needed to compute individual aggregate functions.

## Definition
```c
typedef struct AggStatePerAggData *AggStatePerAgg;
```

Where AggStatePerAggData is defined as:
```c
typedef struct AggStatePerAggData
{
    Aggref     *aggref;
    int         transno;
    Oid         finalfn_oid;
    FmgrInfo    finalfn;
    int         numFinalArgs;
    List       *aggdirectargs;
    int16       resulttypeLen;
    bool        resulttypeByVal;
    bool        shareable;
} AggStatePerAggData;
```

## Detailed Description
AggStatePerAgg is a pointer type that references per-aggregate information structures used during aggregate execution. Each instance contains the metadata and execution state required to compute a specific aggregate function. The structure holds information about the final function, result data type characteristics, and execution parameters. Multiple identical Aggref expressions in a query can share the same AggStatePerAgg instance for efficiency. This design allows the executor to manage aggregate computation state efficiently while supporting complex aggregation scenarios including ordered-set aggregates and shared state optimization.

## Parameters / Member Variables
- `aggref`: Pointer to the Aggref expression node this state represents (first one if multiple identical aggregates exist)
- `transno`: Index to the transition state value that this aggregate should use
- `finalfn_oid`: OID of the final function (may be InvalidOid if no final function)
- `finalfn`: Function manager lookup data for the final function
- `numFinalArgs`: Number of arguments to pass to the final function (at least 1 for transition state)
- `aggdirectargs`: List of ExprState nodes for direct-argument expressions in ordered-set aggregates
- `resulttypeLen`: Length of the aggregate's result data type
- `resulttypeByVal`: Whether the result type is passed by value
- `shareable`: Whether this aggregate can share state with other aggregates (false if final function is read-write)

## Dependencies
- Functions called/Symbols referenced:
  - AggStatePerAggData
  - Aggref
  - [FmgrInfo](../F/FmgrInfo.md)
  - [List](../L/List.md)
- Called from (representative examples):
  - [finalize_aggregate](../f/finalize_aggregate.md)
  - [finalize_partialaggregate](../f/finalize_partialaggregate.md)
  - [finalize_aggregates](../f/finalize_aggregates.md)
  - [agg_retrieve_direct](../a/agg_retrieve_direct.md)
  - [agg_retrieve_hash_table_in_memory](../a/agg_retrieve_hash_table_in_memory.md)
  - [ExecInitAgg](../E/ExecInitAgg.md)
  - [AggGetAggref](AggGetAggref.md)
  - [AggStateIsShared](AggStateIsShared.md)

## Notes and Other Information
- This is a private structure definition in nodeAgg.c, indicating it's an internal implementation detail of the aggregation executor
- The structure is designed to support sharing between identical aggregate expressions for performance optimization
- Contains both compile-time information (set during ExecInitAgg) and runtime execution state
- Supports both regular aggregates and ordered-set aggregates through the aggdirectargs mechanism
- The shareable flag enables optimization by allowing multiple aggregate expressions to share the same transition state when safe