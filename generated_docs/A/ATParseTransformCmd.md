# ATParseTransformCmd

## Location
[src/backend/commands/tablecmds.c:5567-5701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L5567-L5701)

## Overview
ATParseTransformCmd performs parse transformation for ALTER TABLE subcommands, converting raw parsed commands into executable forms and handling the scheduling of generated subcommands across multiple execution passes.

## Definition


## Detailed Description
ATParseTransformCmd serves as a critical transformation layer in PostgreSQL's ALTER TABLE processing pipeline. It takes a single ALTER TABLE subcommand and runs it through the parser's transformation phase, which may generate additional subcommands and utility statements. The function creates a temporary AlterTableStmt containing just the single subcommand, then calls transformAlterTableStmt to perform semantic analysis and generate any necessary additional operations.

The transformation process can produce three types of outputs: before-statements (executed immediately), the transformed subcommand(s), and after-statements (queued for later execution). The function schedules transformed subcommands into appropriate execution passes based on their types and dependencies, ensuring that operations are executed in the correct order to maintain data consistency and constraint integrity.

The function handles sophisticated scheduling logic, determining which pass each generated subcommand should be executed in based on its type and the current pass being processed. Some operations like index constraints and foreign key constraints must be scheduled into later passes to ensure proper dependency handling.

## Parameters / Member Variables
- : Double pointer to the work queue list for managing cascading operations across related tables
- : Pointer to AlteredTableInfo structure containing the table being altered and its scheduling information
- : Relation pointer to the table being altered
- : Pointer to the AlterTableCmd to be transformed
- : Boolean indicating whether the command should recurse to child tables
- : Lock mode to acquire during the operation
- : Current execution pass in the multi-pass ALTER TABLE framework
- : Pointer to AlterTableUtilityContext maintaining context across the ALTER TABLE operation

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [makeRangeVar](../m/makeRangeVar.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - RelationGetNamespace
  - [pstrdup](../p/pstrdup.md)
  - RelationGetRelationName
  - list_make1
  - [transformAlterTableStmt](../t/transformAlterTableStmt.md)
  - RelationGetRelid
  - [ProcessUtilityForAlterTable](../P/ProcessUtilityForAlterTable.md)
  - CommandCounterIncrement
  - lfirst_node
  - [ATPrepSetNotNull](ATPrepSetNotNull.md)
  - castNode
  - lappend
  - [list_concat](../l/list_concat.md)
- Called from:
  - [ATPrepCmd](ATPrepCmd.md)
  - [ATExecCmd](ATExecCmd.md)
  - [ATExecAddColumn](ATExecAddColumn.md)

## Notes and Other Information
- This function is static and only used within the tablecmds.c module
- The transformation process may generate multiple subcommands from a single input command
- Before-statements are executed immediately while after-statements are queued for final execution
- The function implements sophisticated pass scheduling to handle operation dependencies correctly
- Some subcommand types like SET NOT NULL require special preparation via ATPrepSetNotNull
- Index and constraint operations are carefully scheduled into appropriate passes to maintain dependency order
- The function ensures that operations cannot be scheduled into passes that have already been completed
- Returns the transformed version of the original subcommand, or NULL if no direct transformation occurred
- Located at src/backend/commands/tablecmds.c:5567-5701