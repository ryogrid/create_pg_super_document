# AggStatePerPhase

## Location
src/include/nodes/execnodes.h: 2460 - 2460

## Overview
AggStatePerPhase is a typedef pointer to AggStatePerPhaseData structure that represents per-grouping-set-phase state information, managing the execution strategy and configuration for each distinct phase of grouping set processing.

## Definition
```c
typedef struct AggStatePerPhaseData *AggStatePerPhase;
```

Where AggStatePerPhaseData is defined as:
```c
typedef struct AggStatePerPhaseData
{
    AggStrategy aggstrategy;       /* strategy for this phase */
    int         numsets;           /* number of grouping sets (or 0) */
    int        *gset_lengths;      /* lengths of grouping sets */
    Bitmapset **grouped_cols;      /* column groupings for rollup */
    ExprState **eqfunctions;       /* expression returning equality, indexed by
                                    * nr of cols to compare */
    Agg        *aggnode;           /* Agg node for phase data */
    Sort       *sortnode;          /* Sort node for input ordering for phase */
    ExprState  *evaltrans;         /* evaluation of transition functions  */
    ExprState  *evaltrans_cache[2][2]; /* Cached variants of compiled expression */
} AggStatePerPhaseData;
```

## Detailed Description
AggStatePerPhase manages the execution state for individual phases within PostgreSQL's grouping sets implementation. Grouping sets are divided into phases where each phase can be processed in a single pass over the input data. When multiple phases exist, the system processes one phase completely, then resets state and processes the next phase with potentially re-sorted data. Each phase encapsulates its own aggregation strategy, grouping set configuration, equality comparison functions, and compiled expression evaluation state. The structure includes caching mechanisms for compiled expressions to optimize performance across different execution contexts.

## Parameters / Member Variables
- `aggstrategy`: The aggregation strategy (AGG_PLAIN, AGG_SORTED, AGG_HASHED, AGG_MIXED) used for this phase
- `numsets`: Number of grouping sets in this phase (0 for regular aggregation)
- `gset_lengths`: Array containing the lengths of each grouping set in this phase
- `grouped_cols`: Array of Bitmapsets representing column groupings for rollup operations
- `eqfunctions`: Array of ExprState pointers for equality functions, indexed by number of columns to compare
- `aggnode`: Pointer to the Agg plan node containing configuration for this phase
- `sortnode`: Pointer to the Sort plan node defining input ordering requirements for this phase
- `evaltrans`: ExprState for evaluating transition functions in this phase
- `evaltrans_cache`: 2x2 matrix of cached compiled expression variants: [tuple slot type][NULL check presence]

## Dependencies
- Functions called/Symbols referenced:
  - AggStatePerPhaseData
  - AggStrategy
  - [Bitmapset](../B/Bitmapset.md)
  - ExprState
  - Agg
  - Sort
- Called from (representative examples):
  - [ExecBuildAggTrans](../E/ExecBuildAggTrans.md)
  - [hashagg_recompile_expressions](../h/hashagg_recompile_expressions.md)
  - [ExecInitAgg](../E/ExecInitAgg.md)
  - [AggState](AggState.md) (as a member)

## Notes and Other Information
- Central to PostgreSQL's advanced grouping sets functionality (ROLLUP, CUBE, GROUPING SETS)
- Each phase represents a distinct pass over the input data with potentially different sorting and grouping requirements
- The evaltrans_cache matrix optimizes expression evaluation by pre-compiling variants for different execution contexts (different tuple slot types and NULL checking requirements)
- Phases allow complex grouping operations to be broken down into manageable chunks that can be processed sequentially
- The equality functions array enables efficient comparison operations for different numbers of grouping columns
- Multiple phases may require data re-sorting between phases, coordinated through the sortnode configuration
- Essential for implementing SQL standard grouping sets features while maintaining execution efficiency
- The structure supports both simple aggregation (numsets = 0) and complex multi-dimensional grouping scenarios