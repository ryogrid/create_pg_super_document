# ModifyTable

## Location
src/include/nodes/plannodes.h: 229 - 256

## Overview
ModifyTable is a plan node that applies data modification operations (INSERT, UPDATE, DELETE, MERGE) to target tables using rows produced by its outer plan, supporting complex features like partitioning, inheritance, triggers, and conflict resolution.

## Definition


## Detailed Description
ModifyTable is the primary plan node for executing data modification operations in PostgreSQL. It handles the complex orchestration of INSERT, UPDATE, DELETE, and MERGE operations, including support for advanced features like table partitioning, inheritance hierarchies, foreign data wrappers, row-level security, triggers, and conflict resolution.

The node processes rows from its outer plan and applies the specified operation to one or more target tables. For partitioned tables or inheritance hierarchies, it manages the routing of rows to appropriate child tables. It coordinates with the trigger system, handles RETURNING clauses, manages foreign key constraints, and implements PostgreSQL's sophisticated conflict resolution mechanisms.

For MERGE operations, it maintains action lists and join conditions for each target table. For INSERT operations with ON CONFLICT clauses, it manages conflict detection and resolution. The node also integrates with PostgreSQL's concurrency control through EvalPlanQual (EPQ) mechanisms.

## Parameters / Member Variables
- : Base Plan structure with common execution plan fields
- : Type of modification operation (INSERT, UPDATE, DELETE, MERGE)
- : Whether this operation sets the command completion tag
- : Range table index of the relation shown in EXPLAIN output
- : Range table index of root relation for partitioned/inherited tables (0 if not applicable)
- : True if any partitioning key columns are being updated
- : List of range table indexes for all target relations
- : Per-target-table lists of column numbers being updated
- : Per-target-table WITH CHECK OPTION constraint lists
- : Per-target-table RETURNING clause target lists
- : Per-target-table private data for foreign data wrapper operations
- : Bitmap indicating which plans use FDW direct modification
- : Row locking information (for non-locking row marks)
- : Parameter ID for EvalPlanQual re-evaluation during concurrent updates
- : Action to take on INSERT conflicts (IGNORE or UPDATE)
- : List of index OIDs used for ON CONFLICT conflict detection
- : Target list for ON CONFLICT DO UPDATE operations
- : Column numbers targeted by ON CONFLICT DO UPDATE
- : WHERE clause for ON CONFLICT DO UPDATE operations
- : Range table index of the EXCLUDED pseudo-relation for ON CONFLICT
- : Target list of the EXCLUDED pseudo-relation
- : Per-target-table action specifications for MERGE operations
- : Per-target-table join conditions for MERGE operations

## Dependencies
- Functions called/Symbols referenced:
  - Plan (base structure)
  - CmdType
  - OnConflictAction
  - Index
  - List
  - Bitmapset
  - Node

- Called from (representative examples):
  - make_modifytable (optimizer/plan/createplan.c:7040)
  - create_modifytable_plan (optimizer/plan/createplan.c:2817)
  - ExecInitModifyTable (executor/nodeModifyTable.c:4422)
  - ExecInsert (executor/nodeModifyTable.c:793)
  - ExecInitMerge (executor/nodeModifyTable.c:3486)

## Notes and Other Information
- Central node for all data modification operations in PostgreSQL's execution engine
- Handles complex multi-table scenarios including partitioned tables and inheritance hierarchies
- Integrates with PostgreSQL's comprehensive trigger system for BEFORE/AFTER/INSTEAD OF triggers
- Supports sophisticated conflict resolution through ON CONFLICT clauses with IGNORE and UPDATE actions
- Manages RETURNING clauses that can return modified rows to the client
- Coordinates with foreign data wrappers for modifications on remote tables
- Implements EvalPlanQual mechanisms for handling concurrent modifications
- Critical for MERGE statement execution, which combines INSERT, UPDATE, and DELETE logic
- Row-level security policies are enforced through WITH CHECK OPTION mechanisms
- The complexity of this node reflects PostgreSQL's rich feature set for data modification operations