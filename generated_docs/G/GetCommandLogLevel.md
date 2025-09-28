# GetCommandLogLevel

## Location
[src/backend/tcop/utility.c:3247-3768](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L3247-L3768)

## Overview
GetCommandLogLevel is a utility function that determines the minimum log_statement level required for a PostgreSQL command to be logged, supporting raw parse trees, analyzed Queries, and PlannedStmts.

## Definition
```c
LogStmtLevel GetCommandLogLevel(Node *parsetree)
```

## Detailed Description
This function implements PostgreSQL's statement logging classification system by analyzing SQL commands and assigning them to appropriate logging levels. It serves as the core logic for the log_statement configuration parameter, which controls which types of statements are logged based on their operational impact.

The function categorizes commands into three main logging levels:
- LOGSTMT_ALL: Statements that are safe and informational (SELECT, transaction control, etc.)
- LOGSTMT_MOD: Data modification statements (INSERT, UPDATE, DELETE, MERGE, TRUNCATE)
- LOGSTMT_DDL: Data definition statements that modify database structure (CREATE, ALTER, DROP, etc.)

The function recursively processes complex statement types (PREPARE, EXECUTE, EXPLAIN ANALYZE) to determine the logging level of the underlying command. It handles both raw and processed statement forms uniformly for utility commands.

## Parameters / Member Variables
- `parsetree`: A Node pointer representing the command to classify, which can be a raw parse tree, analyzed Query, or PlannedStmt

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to determine the node type)
  - [FetchPreparedStatement](../F/FetchPreparedStatement.md) (for EXECUTE statements)
  - [defGetBoolean](../d/defGetBoolean.md) (for EXPLAIN option processing)
  - [LogStmtLevel](../L/LogStmtLevel.md) constants (LOGSTMT_ALL, LOGSTMT_MOD, LOGSTMT_DDL)
  - Various statement structures (SelectStmt, CopyStmt, etc.)
- Called from (representative examples):
  - [check_log_statement](../c/check_log_statement.md) (src/backend/tcop/postgres.c:2382)
  - [CreateCommandName](../C/CreateCommandName.md) (src/include/tcop/utility.h:108)

## Notes and Other Information
- The function is recursive for RawStmt, PREPARE, EXECUTE, and EXPLAIN ANALYZE statements
- COPY statements are classified as LOGSTMT_MOD when importing (is_from=true) and LOGSTMT_ALL when exporting
- SELECT INTO is classified as LOGSTMT_DDL due to its table creation behavior
- EXPLAIN ANALYZE recursively analyzes the contained statement, while plain EXPLAIN is LOGSTMT_ALL
- For unrecognized node types or command types, it defaults to LOGSTMT_ALL for safety
- The function supports PostgreSQL's hierarchical logging configuration where higher levels include lower levels
- Critical for database auditing and compliance requirements where different statement types require different logging policies

## Simplified Source

```c
// Simplified version of GetCommandLogLevel
LogStmtLevel GetCommandLogLevel(Node *parsetree) {
    LogStmtLevel log_level;

    switch (nodeTag(parsetree)) {
        // Handle wrapped statements recursively
        case T_RawStmt:
            return GetCommandLogLevel(((RawStmt *) parsetree)->stmt);

        // Data modification statements - require MOD level logging
        case T_InsertStmt:
        case T_DeleteStmt:
        case T_UpdateStmt:
        case T_MergeStmt:
        case T_TruncateStmt:
            log_level = LOGSTMT_MOD;
            break;

        // SELECT statements - mostly ALL level, except SELECT INTO
        case T_SelectStmt:
            if (((SelectStmt *) parsetree)->intoClause) {
                log_level = LOGSTMT_DDL;  // SELECT INTO creates tables
            } else {
                log_level = LOGSTMT_ALL;  // Regular SELECT
            }
            break;

        // DDL statements - require DDL level logging
        case T_CreateStmt:
        case T_DropStmt:
        case T_AlterTableStmt:
        case T_IndexStmt:
        case T_CreateSchemaStmt:
        case T_ViewStmt:
        case T_CreateFunctionStmt:
        // ... (many more DDL statement types)
            log_level = LOGSTMT_DDL;
            break;

        // Special handling for COPY - depends on direction
        case T_CopyStmt:
            if (((CopyStmt *) parsetree)->is_from) {
                log_level = LOGSTMT_MOD;  // COPY FROM modifies data
            } else {
                log_level = LOGSTMT_ALL;  // COPY TO just reads data
            }
            break;

        // Handle prepared statements recursively
        case T_PrepareStmt:
            {
                PrepareStmt *prepare_stmt = (PrepareStmt *) parsetree;
                return GetCommandLogLevel(prepare_stmt->query);
            }

        case T_ExecuteStmt:
            {
                ExecuteStmt *execute_stmt = (ExecuteStmt *) parsetree;
                PreparedStatement *prepared = FetchPreparedStatement(execute_stmt->name, false);
                if (prepared && prepared->plansource->raw_parse_tree) {
                    return GetCommandLogLevel(prepared->plansource->raw_parse_tree->stmt);
                } else {
                    log_level = LOGSTMT_ALL;
                }
            }
            break;

        // Handle EXPLAIN with special logic for ANALYZE option
        case T_ExplainStmt:
            {
                ExplainStmt *explain_stmt = (ExplainStmt *) parsetree;
                bool has_analyze = false;

                // Check if ANALYZE option is present
                ListCell *option_cell;
                foreach(option_cell, explain_stmt->options) {
                    DefElem *option = (DefElem *) lfirst(option_cell);
                    if (strcmp(option->defname, "analyze") == 0) {
                        has_analyze = defGetBoolean(option);
                    }
                }

                if (has_analyze) {
                    // EXPLAIN ANALYZE executes the query, so use underlying log level
                    return GetCommandLogLevel(explain_stmt->query);
                } else {
                    // Plain EXPLAIN just shows the plan
                    log_level = LOGSTMT_ALL;
                }
            }
            break;

        // Handle already-planned statements
        case T_PlannedStmt:
            {
                PlannedStmt *planned_stmt = (PlannedStmt *) parsetree;
                switch (planned_stmt->commandType) {
                    case CMD_SELECT:
                        log_level = LOGSTMT_ALL;
                        break;
                    case CMD_UPDATE:
                    case CMD_INSERT:
                    case CMD_DELETE:
                    case CMD_MERGE:
                        log_level = LOGSTMT_MOD;
                        break;
                    case CMD_UTILITY:
                        return GetCommandLogLevel(planned_stmt->utilityStmt);
                    default:
                        log_level = LOGSTMT_ALL;
                        break;
                }
            }
            break;

        // Handle analyzed queries
        case T_Query:
            {
                Query *query = (Query *) parsetree;
                switch (query->commandType) {
                    case CMD_SELECT:
                        log_level = LOGSTMT_ALL;
                        break;
                    case CMD_UPDATE:
                    case CMD_INSERT:
                    case CMD_DELETE:
                    case CMD_MERGE:
                        log_level = LOGSTMT_MOD;
                        break;
                    case CMD_UTILITY:
                        return GetCommandLogLevel(query->utilityStmt);
                    default:
                        log_level = LOGSTMT_ALL;
                        break;
                }
            }
            break;

        // Most utility statements and others default to ALL level
        default:
            log_level = LOGSTMT_ALL;
            break;
    }

    return log_level;
}
```

Key simplifications made:
- Consolidated many similar DDL statement cases into a single comment block
- Added descriptive variable names for clarity
- Focused on the major statement categories and special cases
- Removed extensive case-by-case listing while preserving core logic patterns
- Added comments explaining the classification rationale for each category
- Simplified error handling to use default ALL level