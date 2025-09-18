# make_gather

## Location
[src/backend/optimizer/plan/createplan.c:6855-6883](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6855-L6883)

## Overview
Creates a Gather plan node that coordinates parallel execution by collecting results from multiple worker processes.

## Definition


## Detailed Description
The  function constructs a Gather plan node, which is a crucial component in PostgreSQL's parallel query execution framework. This node acts as a coordinator that spawns worker processes to execute a subplan in parallel and then gathers the results from all workers. The Gather node sits at the boundary between parallel and non-parallel execution, collecting tuples produced by parallel workers and presenting them as a single result stream to the upper levels of the plan tree.

The function initializes all necessary fields of the Gather node, including the target list for output columns, qualification conditions, and parallel execution parameters. It sets up the plan structure with the subplan as the left child and ensures proper initialization of parallel-specific attributes.

## Parameters / Member Variables
- : Target list defining the output columns of the Gather node
- : List of qualification conditions to be applied at this node level
- : Number of parallel worker processes to spawn for executing the subplan
- : Parameter ID used for rescanning in parameterized plans
- : Boolean flag indicating if only one copy of the subplan should be executed (used for certain parallel-unsafe operations)
- : The child plan node that will be executed in parallel by worker processes

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates a new Gather node)
  - Gather (the plan node type being created)
- Called from (representative examples):
  - [create_gather_plan](../c/create_gather_plan.md)
  - CP_IGNORE_TLIST

## Notes and Other Information
- The function is static, indicating it's only used within the createplan.c file
- Sets  to NULL as Gather nodes only have a left child (the parallel subplan)
- Initializes  to false and  to NULL for proper node state
- The  is particularly important for handling parameterized nested loop joins in parallel contexts
- The  parameter is crucial for handling parallel-unsafe operations that should only be executed once across all workers