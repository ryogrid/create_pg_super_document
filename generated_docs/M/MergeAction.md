# MergeAction

## Location
src/include/nodes/primnodes.h: 2003 - 2014

## Overview
MergeAction represents the transformed representation of a WHEN clause in a PostgreSQL MERGE statement, encapsulating the match condition, command type, and associated data for merge operations.

## Definition


## Detailed Description
MergeAction is a node type that represents a transformed WHEN clause within a MERGE statement. Each MergeAction corresponds to one possible action that can be taken based on whether rows match between the source and target tables. The structure encapsulates all the information needed to execute a specific merge action, including the match condition, the type of SQL command to execute, and any associated data such as target lists and column specifications.

The node is used during query planning and execution to determine which action to take for each row processed during the merge operation.

## Parameters / Member Variables
- : Standard NodeTag for node type identification
- : Specifies the match condition (MERGE_WHEN_MATCHED, MERGE_WHEN_NOT_MATCHED_BY_SOURCE, MERGE_WHEN_NOT_MATCHED_BY_TARGET)
- : The SQL command to execute (CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_NOTHING)
- : OVERRIDING clause specification (OVERRIDING_NOT_SET, OVERRIDING_USER_VALUE, OVERRIDING_SYSTEM_VALUE)
- : Transformed WHEN conditions that determine if this action should be executed
- : List of TargetEntry nodes specifying the values for INSERT/UPDATE operations
- : List of target attribute numbers for UPDATE operations (ignored during query jumbling)

## Dependencies
- Functions called/Symbols referenced:
  - MergeMatchKind (enum for match conditions)
  - CmdType (enum for command types)
  - OverridingKind (enum for OVERRIDING clause options)
  - NodeTag (base node identification)
  - List (PostgreSQL list structure)
  
- Called from (representative examples):
  - ExecInitMerge (merge execution initialization)
  - transformMergeStmt (parser transformation of MERGE statements)
  - transform_MERGE_to_join (optimizer transformation to join)
  - get_merge_query_def (rule decompilation for MERGE queries)
  - rewriteTargetView (rewrite system for updateable views)

## Notes and Other Information
- Essential component of PostgreSQL's MERGE statement implementation introduced for SQL standard compliance
- Used extensively in the parser, planner, and executor phases of MERGE statement processing
- The pg_node_attr(query_jumble_ignore) annotations on override and updateColnos indicate these fields should be ignored during query fingerprinting
- Each MERGE statement can have multiple MergeAction nodes, one for each WHEN clause
- Critical for implementing complex data synchronization and upsert operations efficiently
- The match conditions support both traditional MERGE semantics and extended NOT MATCHED BY SOURCE operations