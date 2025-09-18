# JoinPathExtraData

## Location
src/include/nodes/pathnodes.h: 3230 - 3238

## Overview
JoinPathExtraData is a structure used to pass additional information to subroutines of add_paths_to_joinrel, containing join-specific metadata required for path generation and cost estimation.

## Definition


## Detailed Description
JoinPathExtraData serves as a container for essential information needed during join path creation and evaluation. This structure centralizes join-related metadata that multiple functions need access to, avoiding the need to pass numerous individual parameters. It supports various join algorithms (nested loop, merge join, hash join) and special join types (semi, anti joins) by providing context about restrictions, merge conditions, uniqueness properties, and parameterization options.

## Parameters / Member Variables
- : List of RestrictInfo nodes representing all restriction clauses that apply to this specific join operation
- : List of RestrictInfo nodes for available mergejoin clauses that can be used for merge join algorithms
- : Boolean flag indicating whether each outer tuple provably matches no more than one inner tuple (important for optimization decisions)
- : Pointer to SpecialJoinInfo containing extra information about special joins used for selectivity estimation
- : SemiAntiJoinFactors structure containing selectivity factors, only valid for SEMI/ANTI joins and inner_unique joins
- : Relids representing relations that are acceptable targets for parameterization of result paths

## Dependencies
- Functions called/Symbols referenced:
  - SpecialJoinInfo
  - SemiAntiJoinFactors
- Called from (representative examples):
  - add_paths_to_joinrel
  - try_nestloop_path
  - try_mergejoin_path
  - try_hashjoin_path
  - create_nestloop_path
  - create_mergejoin_path
  - create_hashjoin_path
  - initial_cost_nestloop
  - final_cost_nestloop
  - initial_cost_mergejoin
  - final_cost_mergejoin
  - initial_cost_hashjoin
  - final_cost_hashjoin

## Notes and Other Information
- This structure is primarily used in the query optimizer's join planning phase
- The structure helps maintain consistency across different join algorithms by providing a standardized way to pass join context
- The param_source_rels field is crucial for parameterized path generation, enabling nested loop joins with parameters
- The inner_unique flag can significantly impact join cost calculations and algorithm selection
- Located in src/include/nodes/pathnodes.h:3230-3238