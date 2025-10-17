# transformPartitionCmd

## Location
[src/backend/parser/parse_utilcmd.c:3932-3984](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L3932-L3984)

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
  - [RelationGetPartitionKey](../R/RelationGetPartitionKey.md)
  - [transformPartitionBound](transformPartitionBound.md)
  - RelationGetRelationName
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - elog
  - Assert
- Called from (representative examples):
  - [transformAlterTableStmt](transformAlterTableStmt.md) (in src/backend/parser/parse_utilcmd.c:3528)

## Notes and Other Information
- Only handles RELKIND_PARTITIONED_TABLE and RELKIND_PARTITIONED_INDEX as valid parent relations
- Generates specific error messages for different invalid relation types (regular tables, regular indexes)
- For partitioned indexes, partition bounds are not allowed and will trigger an error
- Sets cxt->partbound to the transformed partition bound for successful ATTACH PARTITION operations
- The function enforces PostgreSQL's partitioning rules at the parser level before execution
- Uses Assert to ensure partitioned tables have a valid partition key

## Simplified Source

```c
static void
transformPartitionCmd(CreateStmtContext *cxt, PartitionCmd *cmd)
{
    Relation parentRel = cxt->rel;

    // Validate parent relation type and handle partition bounds
    switch (parentRel->rd_rel->relkind) {
        case RELKIND_PARTITIONED_TABLE:
            // Transform partition bound for ATTACH PARTITION if specified
            Assert(RelationGetPartitionKey(parentRel) != NULL);
            if (cmd->bound != NULL)
                cxt->partbound = transformPartitionBound(cxt->pstate, parentRel, cmd->bound);
            break;

        case RELKIND_PARTITIONED_INDEX:
            // Partitioned indexes cannot have partition bounds
            if (cmd->bound != NULL)
                ereport(ERROR,
                       (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                        errmsg("\"%s\" is not a partitioned table",
                               RelationGetRelationName(parentRel))));
            break;

        case RELKIND_RELATION:
            // Regular table: must be partitioned
            ereport(ERROR,
                   (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("table \"%s\" is not partitioned",
                           RelationGetRelationName(parentRel))));
            break;

        case RELKIND_INDEX:
            // Regular index: must be partitioned
            ereport(ERROR,
                   (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("index \"%s\" is not partitioned",
                           RelationGetRelationName(parentRel))));
            break;

        default:
            // Unexpected relation kind
            elog(ERROR, "\"%s\" is not a partitioned table or index",
                 RelationGetRelationName(parentRel));
            break;
    }
}
```