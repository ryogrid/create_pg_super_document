# ProcessUtilitySlow

## Location
[src/backend/tcop/utility.c:1089-1956](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L1089-L1956)

## Overview
ProcessUtilitySlow is the specialized utility command execution engine that handles DDL statements requiring event trigger support, implementing PostgreSQL's event trigger infrastructure for monitoring and customizing database schema changes.

## Definition

```c
struction **************
				 */
			case T_DefineStmt:
				{
					DefineStmt *stmt = (DefineStmt *) parsetree;

					switch (stmt->kind)
					{
						case OBJECT_AGGREGATE:
							address =
								DefineAggregate(pstate, stmt->defnames, stmt->args,
												stmt->oldstyle,
												stmt->definition,
												stmt->replace);
							break;
						case OBJECT_OPERATOR:
							Assert(stmt->args == NIL);
							address = DefineOperator(stmt->defnames,
													 stmt->definition);
							break;
						case OBJECT_TYPE:
							Assert(stmt->args == NIL);
							address = DefineType(pstate,
												 stmt->defnames,
												 stmt->definition);
							break;
						case OBJECT_TSPARSER:
							Assert(stmt->args == NIL);
							address = DefineTSParser(stmt->defnames,
													 stmt->definition);
							break;
						case OBJECT_TSDICTIONARY:
							Assert(stmt->args == NIL);
							address = DefineTSDictionary(stmt->defnames,
														 stmt->definition);
							break;
						case OBJECT_TSTEMPLATE:
							Assert(stmt->args == NIL);
							address = DefineTSTemplate(stmt->defnames,
													   stmt->definition);
							break;
						case OBJECT_TSCONFIGURATION:
							Assert(stmt->args == NIL);
							address = DefineTSConfiguration(stmt->defnames,
															stmt->definition,
															&secondaryObject);
							break;
						case OBJECT_COLLATION:
							Assert(stmt->args == NIL);
							address = DefineCollation(pstate,
													  stmt->defnames,
													  stmt->definition,
													  stmt->if_not_exists);
							break;
						default:
							elog(ERROR, "unrecognized define stmt type: %d",
								 (int) stmt->kind);
							break;
					}
				}
				break;
```
## Detailed Description
ProcessUtilitySlow serves as PostgreSQL's comprehensive DDL command processor with full event trigger integration. It handles all utility statements that support event triggers, providing sophisticated infrastructure for monitoring, logging, and extending database schema modification operations. The function operates through a complete event trigger lifecycle including command start events, object collection, SQL drop events, and command end events.

The function implements a robust error handling framework using PG_TRY/PG_FINALLY blocks to ensure proper cleanup of event trigger state even when commands fail. It processes an extensive range of DDL operations including table creation/alteration, index management, type definitions, function/procedure creation, view management, access method definitions, publication/subscription management, and many other schema modification commands.

Key architectural features include comprehensive object address tracking for event trigger consumption, command collection mechanisms for dependency tracking, specialized handling for complex multi-statement operations like CREATE TABLE with embedded constraints, and recursive processing capabilities for commands that generate sub-statements.

## Parameters / Member Variables
- `pstate`: ParseState providing parsing context and environment information for statement analysis
- `pstmt`: PlannedStmt wrapper containing the utility statement and execution metadata
- `queryString`: Original source text of the command for event trigger context and error reporting
- `context`: ProcessUtilityContext indicating execution level (toplevel, non-toplevel, or subcommand)
- `params`: ParamListInfo containing parameter values for parameterized statements
- `queryEnv`: QueryEnvironment providing execution environment including ephemeral named tables
- `dest`: DestReceiver specifying destination for command results (typically None_Receiver for DDL)
- `qc`: QueryCompletion structure for tracking command completion status and affected objects

## Dependencies
- Functions called/Symbols referenced:
  - Event trigger infrastructure (EventTriggerBeginCompleteQuery, EventTriggerDDLCommandStart, EventTriggerCollectSimpleCommand, EventTriggerDDLCommandEnd)
  - [Command](../C/Command.md)-specific execution functions (CreateSchemaCommand, DefineRelation, AlterTable, CreateExtension, etc.)
  - Transaction control (PreventInTransactionBlock, CommandCounterIncrement)
  - Parse analysis and transformation functions (transformCreateStmt, transformIndexStmt, transformStatsStmt)
  - Object relationship management (find_all_inheritors, RangeVarGetRelidExtended)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (for commands supporting event triggers)

## Notes and Other Information
- This function is static to utility.c, serving as an internal implementation detail of the utility processing system
- The function guarantees proper event trigger cleanup through PG_TRY/PG_FINALLY exception handling regardless of command success or failure
- Commands are selectively routed here based on EventTriggerSupportsObjectType() checks in standard_ProcessUtility
- Implements sophisticated command collection tracking with commandCollected flags to avoid duplicate event trigger notifications
- Supports complex multi-statement operations through recursive ProcessUtility calls for sub-statements
- Special handling for materialized view refresh includes command collection inhibition to prevent internal DDL from appearing in event triggers
- The function handles both simple single-object commands and complex operations affecting multiple database objects
- Object address tracking enables event triggers to access detailed information about created, modified, or dropped objects
- Supports transaction-level operations like DETACH PARTITION CONCURRENTLY with appropriate transaction block restrictions
- Event trigger integration allows extensions and monitoring tools to intercept and customize DDL operations throughout the PostgreSQL ecosystem

## Simplified Source

```c
static void ProcessUtilitySlow(ParseState *pstate, PlannedStmt *pstmt,
                              const char *queryString, ProcessUtilityContext context,
                              ParamListInfo params, QueryEnvironment *queryEnv,
                              DestReceiver *dest, QueryCompletion *qc) {
    Node *parsetree = pstmt->utilityStmt;
    bool isTopLevel = (context == PROCESS_UTILITY_TOPLEVEL);
    bool isCompleteQuery = (context != PROCESS_UTILITY_SUBCOMMAND);
    bool needCleanup;
    bool commandCollected = false;
    ObjectAddress address;
    ObjectAddress secondaryObject = InvalidObjectAddress;

    // Begin event trigger processing for complete queries
    needCleanup = isCompleteQuery && EventTriggerBeginCompleteQuery();

    PG_TRY();
    {
        // Fire DDL command start event trigger
        if (isCompleteQuery) {
            EventTriggerDDLCommandStart(parsetree);
        }

        // Main command dispatch switch
        switch (nodeTag(parsetree)) {
            case T_CreateSchemaStmt:
                CreateSchemaCommand((CreateSchemaStmt *) parsetree,
                                  queryString, pstmt->stmt_location, pstmt->stmt_len);
                commandCollected = true;
                break;

            case T_CreateStmt:
            case T_CreateForeignTableStmt:
                // Transform and execute table creation statements
                // Handle multiple sub-statements from transformCreateStmt
                // Create tables, foreign tables, process LIKE clauses
                commandCollected = true;
                break;

            case T_AlterTableStmt:
                // Check for transaction restrictions (DETACH CONCURRENTLY)
                // Determine lock mode and acquire relation lock
                // Execute table alteration with event trigger support
                commandCollected = true;
                break;

            case T_AlterDomainStmt:
                // Handle domain alterations: DEFAULT, NOT NULL, constraints
                // Dispatch to appropriate AlterDomain* function
                break;

            case T_DefineStmt:
                // Handle CREATE statements for various object types
                switch (((DefineStmt *) parsetree)->kind) {
                    case OBJECT_AGGREGATE:
                        address = DefineAggregate(/* parameters */);
                        break;
                    case OBJECT_OPERATOR:
                        address = DefineOperator(/* parameters */);
                        break;
                    case OBJECT_TYPE:
                        address = DefineType(/* parameters */);
                        break;
                    // ... other object types
                    default:
                        elog(ERROR, "unrecognized define stmt type");
                        break;
                }
                break;

            case T_IndexStmt:
                // Handle CREATE INDEX with concurrency checks
                // Lock relation and validate partitioning constraints
                // Transform and execute index creation
                commandCollected = true;
                break;

            case T_CreateFunctionStmt:
                address = CreateFunction(pstate, (CreateFunctionStmt *) parsetree);
                break;

            case T_ViewStmt:
                address = DefineView(/* parameters */);
                commandCollected = true;
                break;

            case T_DropStmt:
                ExecDropStmt((DropStmt *) parsetree, isTopLevel);
                commandCollected = true;
                break;

            // ... many other DDL statement types (extension, FDW, triggers,
            // publications, subscriptions, statistics, etc.)

            default:
                elog(ERROR, "unrecognized node type: %d", (int) nodeTag(parsetree));
                break;
        }

        // Collect command for event triggers if not already done
        if (!commandCollected) {
            EventTriggerCollectSimpleCommand(address, secondaryObject, parsetree);
        }

        // Fire end event triggers for complete queries
        if (isCompleteQuery) {
            EventTriggerSQLDrop(parsetree);
            EventTriggerDDLCommandEnd(parsetree);
        }
    }
    PG_FINALLY();
    {
        // Ensure event trigger cleanup even on errors
        if (needCleanup) {
            EventTriggerEndCompleteQuery();
        }
    }
    PG_END_TRY();
}
```