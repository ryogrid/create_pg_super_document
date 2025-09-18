# ATExecCmd

## Location
src/backend/commands/tablecmds.c: 5232 - 5566

## Overview
ATExecCmd is the central dispatcher function that executes individual ALTER TABLE subcommands by routing each command to its appropriate execution routine based on the command subtype.

## Definition


## Detailed Description
ATExecCmd serves as the main execution dispatcher for ALTER TABLE operations in PostgreSQL. It uses a large switch statement to route different ALTER TABLE subcommands (identified by cmd->subtype) to their specific execution functions. The function handles over 60 different ALTER TABLE subcommand types, including column operations (ADD, DROP, ALTER), constraint operations, index operations, trigger/rule management, inheritance, partitioning, and table properties.

The function operates within PostgreSQL's multi-pass ALTER TABLE framework, where complex ALTER TABLE statements are broken down into multiple passes to handle dependencies correctly. Some operations require transformation via ATParseTransformCmd before execution, particularly for constraints and partitioning operations.

After executing each subcommand, the function reports the operation to event triggers and increments the command counter to ensure subsequent commands can see the changes made by the current command.

## Parameters / Member Variables
- : Double pointer to the work queue list for managing dependent operations and cascading changes
- : Pointer to AlteredTableInfo structure containing information about the table being altered
- : Pointer to AlterTableCmd structure containing the specific subcommand to execute
- : The lock mode to acquire on the relation during the operation
- : The current pass of the ALTER TABLE operation (for multi-pass execution)
- : Pointer to AlterTableUtilityContext for maintaining context across operations

## Dependencies
- Functions called/Symbols referenced:
  - ATExecAddColumn
  - ATExecColumnDefault
  - ATExecCookedColumnDefault
  - ATExecAddIdentity
  - ATExecSetIdentity
  - ATExecDropIdentity
  - ATExecDropNotNull
  - ATExecSetNotNull
  - ATExecCheckNotNull
  - ATExecSetExpression
  - ATExecDropExpression
  - ATExecSetStatistics
  - ATExecSetOptions
  - ATExecSetStorage
  - ATExecSetCompression
  - ATExecDropColumn
  - ATExecAddIndex
  - ATExecAddStatistics
  - ATExecAddConstraint
  - ATExecAddIndexConstraint
  - ATExecAlterConstraint
  - ATExecValidateConstraint
  - ATExecDropConstraint
  - ATExecAlterColumnType
  - ATExecAlterColumnGenericOptions
  - ATExecChangeOwner
  - ATExecClusterOn
  - ATExecDropCluster
  - ATExecSetAccessMethodNoStorage
  - ATExecSetTableSpaceNoStorage
  - ATExecSetRelOptions
  - ATExecEnableDisableTrigger
  - ATExecEnableDisableRule
  - ATExecAddInherit
  - ATExecDropInherit
  - ATExecAddOf
  - ATExecDropOf
  - ATExecReplicaIdentity
  - ATExecSetRowSecurity
  - ATExecForceNoForceRowSecurity
  - ATExecGenericOptions
  - ATExecAttachPartition
  - ATExecAttachPartitionIdx
  - ATExecDetachPartition
  - ATExecDetachPartitionFinalize
  - ATParseTransformCmd
  - AlterDomainAddConstraint
  - CommentObject
  - get_rolespec_oid
  - EventTriggerCollectAlterTableSubcmd
  - CommandCounterIncrement
- Called from:
  - ATRewriteCatalogs

## Notes and Other Information
- This function is static and only used within the tablecmds.c module
- The function handles a comprehensive set of ALTER TABLE operations through a single switch statement with over 60 cases
- Some operations like SET LOGGED/UNLOGGED and DROP OIDS are handled as no-ops in certain contexts
- The function ensures proper command counter incrementation after each operation to maintain transaction visibility
- Event trigger reporting is performed for each executed subcommand to maintain proper DDL auditing
- Located at src/backend/commands/tablecmds.c:5232-5566