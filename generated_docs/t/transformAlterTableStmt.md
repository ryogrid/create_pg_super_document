# transformAlterTableStmt

## Location
src/backend/parser/parse_utilcmd.c: 3273 - 3636

## Overview
Performs comprehensive parse analysis for ALTER TABLE statements, transforming various subcommands and generating additional statements needed before and after the main alteration.

## Definition
AlterTableStmt *transformAlterTableStmt(Oid relid, AlterTableStmt *stmt, const char *queryString, List **beforeStmts, List **afterStmts)

## Detailed Description
The transformAlterTableStmt function handles the complex transformation of ALTER TABLE statements, which can involve multiple types of table modifications. Its comprehensive responsibilities include:

1. **Parse State and Context Setup**: Creates a ParseState for expression parsing and establishes a CreateStmtContext to manage the transformation process, handling both regular tables and foreign tables appropriately.

2. **Subcommand Processing**: Processes various ALTER TABLE subcommands including:
   - **AT_AddColumn**: Transforms new column definitions, processes constraints, and determines if foreign key validation can be skipped
   - **AT_AddConstraint**: Handles constraint additions and determines validation requirements
   - **AT_AlterColumnType**: Processes column type changes, transforms USING clauses, and handles identity column sequence updates
   - **AT_AddIdentity/AT_SetIdentity**: Manages identity column creation and modification, including associated sequence operations
   - **AT_AttachPartition/AT_DetachPartition**: Handles partition management operations

3. **Identity Column Handling**: For identity columns, generates appropriate ALTER SEQUENCE statements to maintain sequence consistency during type changes or identity modifications.

4. **Constraint Processing**: After processing subcommands, transforms various constraint types:
   - Index constraints via transformIndexConstraints
   - Foreign key constraints via transformFKConstraints  
   - Check constraints via transformCheckConstraints

5. **Index Management**: Processes index-creation commands, ensuring they are properly transformed through transformIndexStmt and converted to appropriate ALTER TABLE subcommands (AT_AddIndex or AT_AddIndexConstraint).

6. **Statement Orchestration**: Organizes the transformation results into three categories:
   - Commands that must execute before the main ALTER TABLE
   - The transformed ALTER TABLE statement itself
   - Commands that must execute after the main ALTER TABLE

The function ensures race condition safety by relying on the passed relid rather than the statement's relation field, and handles complex dependencies between different types of alterations.

## Parameters / Member Variables
- : Object identifier of the relation being altered
- : AlterTableStmt structure containing the parsed ALTER TABLE command to be transformed
- : Original SQL query string used for error reporting and expression transformation
- : Output parameter receiving list of statements to execute before the main ALTER TABLE
- : Output parameter receiving list of statements to execute after the main ALTER TABLE

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md)
  - [make_parsestate](../m/make_parsestate.md)
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md)
  - [addNSItemToQuery](../a/addNSItemToQuery.md)
  - [transformColumnDefinition](transformColumnDefinition.md)
  - [transformTableConstraint](transformTableConstraint.md)
  - [transformExpr](transformExpr.md)
  - [get_attnum](../g/get_attnum.md)
  - [getIdentitySequence](../g/getIdentitySequence.md)
  - [typenameTypeId](typenameTypeId.md)
  - [generateSerialExtraStmts](../g/generateSerialExtraStmts.md)
  - [transformPartitionCmd](transformPartitionCmd.md)
  - [transformIndexConstraints](transformIndexConstraints.md)
  - [transformFKConstraints](transformFKConstraints.md)
  - [transformCheckConstraints](transformCheckConstraints.md)
  - [transformIndexStmt](transformIndexStmt.md)
  - [relation_close](../r/relation_close.md)
- Called from (representative examples):
  - [ATParseTransformCmd](../A/ATParseTransformCmd.md)
  - [ATPostAlterTypeParse](../A/ATPostAlterTypeParse.md)

## Notes and Other Information
- Central function for ALTER TABLE statement processing in PostgreSQL's utility command system
- Handles complex orchestration of multiple statement types that may be needed for a single ALTER TABLE
- Identity column support includes automatic sequence management for type changes
- Foreign key validation can be optimized when certain conditions are met (no new non-null defaults)
- The function distinguishes between regular tables and foreign tables for appropriate handling
- Constraint processing is deferred until all subcommands are initially processed
- Index statements generated from constraints are automatically transformed and integrated
- Race condition safety is maintained through consistent use of relid parameter
- The three-phase execution model (before/main/after) ensures proper dependency ordering
- Partition operations receive special handling through dedicated transformation functions
- The function is essential for maintaining data integrity during complex table modifications