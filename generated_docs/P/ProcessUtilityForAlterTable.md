# ProcessUtilityForAlterTable

## Location
[src/backend/tcop/utility.c:1957-1992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L1957-L1992)

## Overview
ProcessUtilityForAlterTable is a specialized recursive entry point for executing utility subcommands generated during ALTER TABLE operations, ensuring proper event trigger sequencing and context management.

## Definition

```c
void
ProcessUtilityForAlterTable(Node *stmt, AlterTableUtilityContext *context)
```
## Detailed Description
ProcessUtilityForAlterTable serves as a specialized gateway for executing utility commands that are dynamically generated as part of ALTER TABLE processing. When ALTER TABLE operations need to create subsidiary objects like indexes, constraints, or triggers, they use this function rather than the main ProcessUtility entry point to ensure proper integration with PostgreSQL's event trigger system and maintain correct command ordering.

The function implements a sophisticated event trigger management protocol, carefully coordinating the closure of the current ALTER TABLE event trigger context before executing the subcommand, then re-establishing the ALTER TABLE context afterward. This ensures that event triggers see subcommands in the correct sequence and with appropriate context information.

The function creates a complete PlannedStmt wrapper for the subcommand, inheriting location and length information from the parent ALTER TABLE statement to maintain proper source mapping for error reporting and debugging. It executes the subcommand as a PROCESS_UTILITY_SUBCOMMAND, indicating its subsidiary nature within the larger ALTER TABLE operation.

## Parameters / Member Variables
- `stmt`: The Node representing the utility subcommand to execute (e.g., CreateStmt for indexes, ConstraintStmt for constraints)
- `context`: AlterTableUtilityContext structure containing all necessary context from the parent ALTER TABLE operation, including the original PlannedStmt, query string, target relation OID, parameters, and query environment

## Dependencies
- Functions called/Symbols referenced:
  - [ProcessUtility](ProcessUtility.md) (main utility command dispatcher for executing the wrapped subcommand)
  - [EventTriggerAlterTableEnd](../E/EventTriggerAlterTableEnd.md) (closes current ALTER TABLE event trigger context)
  - [EventTriggerAlterTableStart](../E/EventTriggerAlterTableStart.md) (re-establishes ALTER TABLE event trigger context)
  - [EventTriggerAlterTableRelid](../E/EventTriggerAlterTableRelid.md) (sets the target relation for event trigger context)
  - makeNode (creates the PlannedStmt wrapper)
- Called from (representative examples):
  - [ATParseTransformCmd](../A/ATParseTransformCmd.md) (during ALTER TABLE command parsing and transformation)
  - [ATRewriteTables](../A/ATRewriteTables.md) (during ALTER TABLE table rewriting phase)

## Notes and Other Information
- This function is specifically designed for ALTER TABLE's internal command generation and should not be used for other utility command scenarios
- The caller is responsible for calling CommandCounterIncrement after this function returns if needed for visibility of changes
- The function uses None_Receiver as the destination since ALTER TABLE subcommands typically don't produce result sets for client consumption
- Event trigger coordination is critical: the function ensures subcommands appear in event trigger logs with proper sequencing relative to the main ALTER TABLE command
- The PlannedStmt wrapper inherits stmt_location and stmt_len from the parent command to maintain source code mapping for error reporting
- Context preservation allows subcommands to access the same parameters and query environment as the parent ALTER TABLE operation
- The PROCESS_UTILITY_SUBCOMMAND context ensures that subcommands are treated appropriately by the utility processing system
- This design enables ALTER TABLE to generate complex sequences of DDL operations while maintaining transactional consistency and proper event trigger semantics