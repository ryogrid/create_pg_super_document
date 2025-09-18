# transformPartitionCmd

## Location
src/backend/parser/parse_utilcmd.c: 3932 - 3984

## Overview
Analyzes and validates ATTACH/DETACH PARTITION commands, ensuring the parent relation is appropriately partitioned and transforming partition bounds when specified.

## Definition
```c
static void transformPartitionCmd(CreateStmtContext *cxt, PartitionCmd *cmd)
```

## Detailed Description
This static function processes ATTACH/DETACH PARTITION commands by validating that the parent relation is properly configured for partitioning operations. It checks the relation kind of the parent relation and handles partition bound transformation for ATTACH PARTITION commands. For partitioned tables, it transforms the partition bound specification if provided. The function enforces partitioning constraints by rejecting operations on non-partitioned relations and preventing partition bounds on partitioned indexes.

When processing an ATTACH PARTITION command with a partition bound, the function calls transformPartitionBound to convert the raw partition bound specification into a validated, transformed representation that gets stored in the context for later use.

## Parameters / Member Variables
- `cxt`: CreateStmtContext containing the statement parsing context, including the parent relation and where the transformed partition bound will be stored
- `cmd`: PartitionCmd structure containing the partition command details, including the optional partition bound specification

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetPartitionKey
  - transformPartitionBound
  - RelationGetRelationName
  - ereport
  - errcode
  - errmsg
  - elog
  - Assert
- Called from (representative examples):
  - transformAlterTableStmt (in src/backend/parser/parse_utilcmd.c:3528)

## Notes and Other Information
- Only handles RELKIND_PARTITIONED_TABLE and RELKIND_PARTITIONED_INDEX as valid parent relations
- Generates specific error messages for different invalid relation types (regular tables, regular indexes)
- For partitioned indexes, partition bounds are not allowed and will trigger an error
- Sets cxt->partbound to the transformed partition bound for successful ATTACH PARTITION operations
- The function enforces PostgreSQL's partitioning rules at the parser level before execution
- Uses Assert to ensure partitioned tables have a valid partition key