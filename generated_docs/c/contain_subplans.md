# contain_subplans

## Location
src/backend/optimizer/util/clauses.c: 330 - 335

## Overview
Recursively searches for subplan nodes within a clause, returning true if any subplan is found.

## Definition


## Detailed Description
This function serves as a wrapper around  to detect the presence of subplans within an expression tree. It performs a recursive search through the given clause to identify any SubLink nodes, which indicate the presence of subqueries that will likely become subplans during query execution.

The function makes a conservative assumption that any SubLink node encountered will produce a subplan, even though it could potentially become just an initplan. This conservative approach helps the query planner make safer decisions about query optimization strategies.

This function is typically used during the early stages of query planning, before the expression tree has been transformed by , when SubLink nodes are still present in their original form.

## Parameters / Member Variables
- : The expression node to search for subplan references

## Dependencies
- Functions called/Symbols referenced:
  - [contain_subplans_walker](contain_subplans_walker.md)
- Called from (representative examples):
  - [ExecInitValuesScan](../E/ExecInitValuesScan.md)
  - initialize_peragg
  - [find_window_run_conditions](../f/find_window_run_conditions.md)
  - [qual_is_pushdown_safe](../q/qual_is_pushdown_safe.md)
  - [subquery_planner](../s/subquery_planner.md)
  - [convert_EXISTS_to_ANY](convert_EXISTS_to_ANY.md)
  - [inline_function](../i/inline_function.md)
  - [inline_set_returning_function](../i/inline_set_returning_function.md)

## Notes and Other Information
- This is a simple wrapper function that delegates the actual work to 
- Used primarily during query planning to determine optimization strategies
- Makes conservative assumptions about SubLink nodes becoming subplans
- Part of PostgreSQL's subplan detection and manipulation subsystem
- Returns true if any subplan is found, false otherwise