# AppendPath

## Location
src/include/nodes/pathnodes.h: 1931 - 1938

## Overview
AppendPath represents an execution plan for successive execution of multiple member subpaths, commonly used for partitioned tables, UNION operations, and cases where multiple access methods need to be combined.

## Definition


## Detailed Description
AppendPath is a fundamental path node in PostgreSQL that enables the combination of multiple execution paths into a single sequential execution plan. This path type is essential for implementing partitioned table access, UNION operations, and scenarios where multiple different scan strategies need to be executed in sequence.

The AppendPath supports both regular and parallel execution modes. For parallel execution, it can handle mixed workloads where some subpaths are executed by individual workers (partial paths) while others are executed normally. The path automatically optimizes the order of subpath execution to minimize total execution time.

A key feature is support for "dummy" AppendPaths with no subpaths, representing provably empty relations. This optimization allows the planner to recognize when constraint exclusion has eliminated all possible data sources without requiring special case handling throughout the planning process.

## Parameters / Member Variables
- : Base Path structure containing standard path information including pathtype (T_Append), cost estimates, parallel execution properties, and pathkeys for ordered results
- : List of component Path nodes to be executed sequentially, with non-partial paths listed before partial paths in parallel mode
- : Index marking the first partial path in the subpaths list, or list_length(subpaths) if no partial paths exist
- : Hard limit on output tuples when query-wide LIMIT applies to sole base relation, or -1 if no limit

## Dependencies
- Functions called/Symbols referenced:
  - Path (base structure)
  - List (for subpaths storage)  
  - Cardinality (for limit_tuples)
- Called from (representative examples):
  - create_append_path (creates AppendPath instances)
  - create_append_plan (converts AppendPath to execution plan)
  - add_paths_to_append_rel (builds paths for partitioned relations)
  - cost_append (calculates execution costs)

## Notes and Other Information
- Supports both ordered and unordered execution based on pathkeys
- Can represent empty relations through zero-length subpaths list (dummy paths)
- Optimizes single-subpath cases by inheriting child properties directly
- Handles parallel execution with automatic work distribution between workers and leader
- Supports asynchronous execution for improved performance with foreign tables
- Used extensively for partitioned table access and constraint exclusion
- Can apply query-wide LIMIT optimizations when appropriate
- Supports runtime partition pruning through partition pruning info
- Subpaths are sorted by cost in parallel mode for optimal work distribution
- Essential component for implementing UNION, partitioned tables, and append-only optimizations