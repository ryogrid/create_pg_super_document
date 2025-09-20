# JoinPathExtraData

## Location
[src/include/nodes/pathnodes.h:3230-3238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L3230-L3238)

## Overview
JoinPathExtraData is a structure used to pass additional information to subroutines of add_paths_to_joinrel, containing join-specific metadata required for path generation and cost estimation.

## Definition

```c
typedef struct JoinPathExtraData
{
	List	   *restrictlist;
	List	   *mergeclause_list;
	bool		inner_unique;
	SpecialJoinInfo *sjinfo;
	SemiAntiJoinFactors semifactors;
	Relids		param_source_rels;
} JoinPathExtraData;
```
## Detailed Description
JoinPathExtraData serves as a container for essential information needed during join path creation and evaluation. This structure centralizes join-related metadata that multiple functions need access to, avoiding the need to pass numerous individual parameters. It supports various join algorithms (nested loop, merge join, hash join) and special join types (semi, anti joins) by providing context about restrictions, merge conditions, uniqueness properties, and parameterization options.

## Parameters / Member Variables
- `*restrictlist`: List of RestrictInfo nodes representing all restriction clauses that apply to this specific join operation
- `*mergeclause_list`: List of RestrictInfo nodes for available mergejoin clauses that can be used for merge join algorithms
- `inner_unique`: Boolean flag indicating whether each outer tuple provably matches no more than one inner tuple (important for optimization decisions)
- `*sjinfo`: Pointer to SpecialJoinInfo containing extra information about special joins used for selectivity estimation
- `semifactors`: SemiAntiJoinFactors structure containing selectivity factors, only valid for SEMI/ANTI joins and inner_unique joins
- `param_source_rels`: Relids representing relations that are acceptable targets for parameterization of result paths
## Dependencies
- Functions called/Symbols referenced:
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md)
  - SemiAntiJoinFactors
- Called from (representative examples):
  - [add_paths_to_joinrel](../a/add_paths_to_joinrel.md)
  - [try_nestloop_path](../t/try_nestloop_path.md)
  - [try_mergejoin_path](../t/try_mergejoin_path.md)
  - [try_hashjoin_path](../t/try_hashjoin_path.md)
  - [create_nestloop_path](../c/create_nestloop_path.md)
  - [create_mergejoin_path](../c/create_mergejoin_path.md)
  - [create_hashjoin_path](../c/create_hashjoin_path.md)
  - [initial_cost_nestloop](../i/initial_cost_nestloop.md)
  - [final_cost_nestloop](../f/final_cost_nestloop.md)
  - [initial_cost_mergejoin](../i/initial_cost_mergejoin.md)
  - [final_cost_mergejoin](../f/final_cost_mergejoin.md)
  - [initial_cost_hashjoin](../i/initial_cost_hashjoin.md)
  - [final_cost_hashjoin](../f/final_cost_hashjoin.md)

## Notes and Other Information
- This structure is primarily used in the query optimizer's join planning phase
- The structure helps maintain consistency across different join algorithms by providing a standardized way to pass join context
- The param_source_rels field is crucial for parameterized path generation, enabling nested loop joins with parameters
- The inner_unique flag can significantly impact join cost calculations and algorithm selection
- Located in src/include/nodes/pathnodes.h:3230-3238