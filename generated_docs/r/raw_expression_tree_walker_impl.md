# raw_expression_tree_walker_impl

## Location
src/backend/nodes/nodeFuncs.c: 3964 - 4675

## Overview
A comprehensive tree walker function that traverses raw parse trees (pre-analysis) for DML statements, handling all node types found in raw grammar output.

## Definition


## Detailed Description
The  function provides tree traversal capabilities for raw parse trees, which are the direct output of the PostgreSQL grammar parser before semantic analysis. Unlike the regular , this function operates on unprocessed syntax trees and includes handling for all node types that can appear in raw DML statements (SELECT/INSERT/UPDATE/DELETE/MERGE).

The function implements a comprehensive switch statement covering over 60 different node types, from primitive literals and expressions to complex statement structures. It recursively walks through sub-nodes using the  macro, respecting the structure of each node type. The function includes extensive support for JSON operations, table functions, CTEs, and various SQL constructs.

This walker is particularly important during CTE analysis and other early-stage query processing where the system needs to examine raw parse tree structures before they undergo semantic transformation.

## Parameters / Member Variables
- : The root node of the raw parse tree to traverse
- : Callback function that defines the walking behavior for each visited node
- : Opaque context pointer passed through to the walker callback

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - nodeTag (node type identification macro)
  - WALK (recursive traversal macro)
  - elog (error logging for unrecognized nodes)
  - Various node type constants (T_JsonFormat, T_SelectStmt, etc.)
- Called from (representative examples):
  - raw_expression_tree_walker (wrapper function)
  - planstate_tree_walker (indirectly via wrapper)

## Notes and Other Information
- Returns boolean indicating whether the walk should terminate early (true) or continue (false)
- Unlike , this function has no special query boundary rules and descends into all potentially interesting nodes
- Covers extensive JSON functionality including JSON path expressions, JSON table functions, and JSON constructors
- Includes stack depth checking to prevent overflow on deeply nested expressions
- Node type coverage is specifically focused on DML statements as these are the primary use case for CTE analysis
- Primitive node types (literals, constants, parameters) are handled as leaf nodes with no further traversal
- Located in src/backend/nodes/nodeFuncs.c:3964-4675