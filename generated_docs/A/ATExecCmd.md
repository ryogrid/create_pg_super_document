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
  - [ATExecAddColumn](ATExecAddColumn.md)
  - [ATExecColumnDefault](ATExecColumnDefault.md)
  - [ATExecCookedColumnDefault](ATExecCookedColumnDefault.md)
  - [ATExecAddIdentity](ATExecAddIdentity.md)
  - [ATExecSetIdentity](ATExecSetIdentity.md)
  - [ATExecDropIdentity](ATExecDropIdentity.md)
  - [ATExecDropNotNull](ATExecDropNotNull.md)
  - [ATExecSetNotNull](ATExecSetNotNull.md)
  - [ATExecCheckNotNull](ATExecCheckNotNull.md)
  - [ATExecSetExpression](ATExecSetExpression.md)
  - [ATExecDropExpression](ATExecDropExpression.md)
  - [ATExecSetStatistics](ATExecSetStatistics.md)
  - [ATExecSetOptions](ATExecSetOptions.md)
  - [ATExecSetStorage](ATExecSetStorage.md)
  - [ATExecSetCompression](ATExecSetCompression.md)
  - [ATExecDropColumn](ATExecDropColumn.md)
  - [ATExecAddIndex](ATExecAddIndex.md)
  - [ATExecAddStatistics](ATExecAddStatistics.md)
  - [ATExecAddConstraint](ATExecAddConstraint.md)
  - [ATExecAddIndexConstraint](ATExecAddIndexConstraint.md)
  - [ATExecAlterConstraint](ATExecAlterConstraint.md)
  - [ATExecValidateConstraint](ATExecValidateConstraint.md)
  - [ATExecDropConstraint](ATExecDropConstraint.md)
  - [ATExecAlterColumnType](ATExecAlterColumnType.md)
  - [ATExecAlterColumnGenericOptions](ATExecAlterColumnGenericOptions.md)
  - [ATExecChangeOwner](ATExecChangeOwner.md)
  - [ATExecClusterOn](ATExecClusterOn.md)
  - [ATExecDropCluster](ATExecDropCluster.md)
  - [ATExecSetAccessMethodNoStorage](ATExecSetAccessMethodNoStorage.md)
  - [ATExecSetTableSpaceNoStorage](ATExecSetTableSpaceNoStorage.md)
  - [ATExecSetRelOptions](ATExecSetRelOptions.md)
  - [ATExecEnableDisableTrigger](ATExecEnableDisableTrigger.md)
  - [ATExecEnableDisableRule](ATExecEnableDisableRule.md)
  - [ATExecAddInherit](ATExecAddInherit.md)
  - [ATExecDropInherit](ATExecDropInherit.md)
  - [ATExecAddOf](ATExecAddOf.md)
  - [ATExecDropOf](ATExecDropOf.md)
  - [ATExecReplicaIdentity](ATExecReplicaIdentity.md)
  - [ATExecSetRowSecurity](ATExecSetRowSecurity.md)
  - [ATExecForceNoForceRowSecurity](ATExecForceNoForceRowSecurity.md)
  - [ATExecGenericOptions](ATExecGenericOptions.md)
  - [ATExecAttachPartition](ATExecAttachPartition.md)
  - [ATExecAttachPartitionIdx](ATExecAttachPartitionIdx.md)
  - [ATExecDetachPartition](ATExecDetachPartition.md)
  - [ATExecDetachPartitionFinalize](ATExecDetachPartitionFinalize.md)
  - [ATParseTransformCmd](ATParseTransformCmd.md)
  - [AlterDomainAddConstraint](AlterDomainAddConstraint.md)
  - [CommentObject](../C/CommentObject.md)
  - get_rolespec_oid
  - [EventTriggerCollectAlterTableSubcmd](../E/EventTriggerCollectAlterTableSubcmd.md)
  - CommandCounterIncrement
- Called from:
  - [ATRewriteCatalogs](ATRewriteCatalogs.md)

## Notes and Other Information
- This function is static and only used within the tablecmds.c module
- The function handles a comprehensive set of ALTER TABLE operations through a single switch statement with over 60 cases
- Some operations like SET LOGGED/UNLOGGED and DROP OIDS are handled as no-ops in certain contexts
- The function ensures proper command counter incrementation after each operation to maintain transaction visibility
- Event trigger reporting is performed for each executed subcommand to maintain proper DDL auditing
- Located at src/backend/commands/tablecmds.c:5232-5566