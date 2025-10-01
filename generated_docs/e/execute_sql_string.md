# execute_sql_string

## Location
[src/backend/commands/extension.c:741-847](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L741-L847)

## Overview
Executes a multi-statement SQL string by parsing, analyzing, planning, and executing each statement sequentially to handle interdependencies between statements.

## Definition
```c
static void execute_sql_string(const char *sql)
```

## Detailed Description
This function executes SQL strings that may contain multiple statements, particularly for extension script execution. Unlike using SPI (Server Programming Interface), this function handles complex scenarios where statements may have interdependencies - for example, when later statements reference objects created by earlier statements in the same script.

The function implements a sophisticated execution strategy:
1. Parses the entire SQL string into individual raw parse trees
2. Processes each parse tree individually in sequence
3. For each statement, performs full parse analysis, rewrite, planning, and execution before moving to the next
4. Uses memory contexts to limit memory usage during execution
5. Handles both regular queries and utility statements (DDL commands)
6. Specifically prohibits transaction control statements within extension scripts

Key design decisions include avoiding SPI to prevent issues with forward references and using CommandCounterIncrement() to ensure DDL visibility between statements.

## Parameters / Member Variables
- `sql`: The SQL string containing one or more statements to execute

## Dependencies
- Functions called/Symbols referenced:
  - [pg_parse_query](../p/pg_parse_query.md) (parses SQL string into parse trees)
  - [CreateDestReceiver](../C/CreateDestReceiver.md) (creates output destination)
  - AllocSetContextCreate (creates memory context)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md) (makes DDL changes visible)
  - [pg_analyze_and_rewrite_fixedparams](../p/pg_analyze_and_rewrite_fixedparams.md) (analyzes and rewrites queries)
  - [pg_plan_queries](../p/pg_plan_queries.md) (creates execution plans)
  - [CreateQueryDesc](../C/CreateQueryDesc.md) (creates query descriptor)
  - [ExecutorStart](../E/ExecutorStart.md)/ExecutorRun/ExecutorFinish/ExecutorEnd (query execution)
  - [ProcessUtility](../P/ProcessUtility.md) (executes utility statements)
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md)/PopActiveSnapshot (snapshot management)
- Called from:
  - [execute_extension_script](execute_extension_script.md)

## Notes and Other Information
- This is a static function within the extension.c module
- Explicitly avoids using SPI due to limitations with forward references and error reporting
- Uses DestNone to discard SELECT output during script execution
- Prohibits transaction control statements (BEGIN, COMMIT, ROLLBACK) in extension scripts
- Memory management uses per-statement contexts to prevent excessive memory usage
- Essential for proper extension installation where scripts may contain interdependent DDL statements
- The sequential execution model ensures that each statement sees the effects of previous statements

## Simplified Source

```c
static void execute_sql_string(const char *sql) {
    // Parse SQL string into individual statements
    List *raw_parsetree_list = pg_parse_query(sql);

    // Discard all SELECT output
    DestReceiver *dest = CreateDestReceiver(DestNone);

    // Process each statement sequentially
    foreach(lc1, raw_parsetree_list) {
        RawStmt *parsetree = lfirst_node(RawStmt, lc1);

        // Create per-statement memory context
        MemoryContext per_stmt_context = AllocSetContextCreate(
            CurrentMemoryContext,
            "execute_sql_string per-statement context",
            ALLOCSET_DEFAULT_SIZES);
        MemoryContext oldcontext = MemoryContextSwitchTo(per_stmt_context);

        // Make previous DDL changes visible
        CommandCounterIncrement();

        // Analyze, rewrite, and plan the statement
        List *stmt_list = pg_analyze_and_rewrite_fixedparams(parsetree, sql, NULL, 0, NULL);
        stmt_list = pg_plan_queries(stmt_list, sql, CURSOR_OPT_PARALLEL_OK, NULL);

        // Execute each planned statement
        foreach(lc2, stmt_list) {
            PlannedStmt *stmt = lfirst_node(PlannedStmt, lc2);

            CommandCounterIncrement();
            PushActiveSnapshot(GetTransactionSnapshot());

            if (stmt->utilityStmt == NULL) {
                // Execute regular query
                QueryDesc *qdesc = CreateQueryDesc(stmt, sql, GetActiveSnapshot(),
                                                  NULL, dest, NULL, NULL, 0);
                ExecutorStart(qdesc, 0);
                ExecutorRun(qdesc, ForwardScanDirection, 0, true);
                ExecutorFinish(qdesc);
                ExecutorEnd(qdesc);
                FreeQueryDesc(qdesc);
            } else {
                // Execute utility statement (DDL)
                if (IsA(stmt->utilityStmt, TransactionStmt))
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                   errmsg("transaction control statements are not allowed within an extension script")));

                ProcessUtility(stmt, sql, false, PROCESS_UTILITY_QUERY,
                             NULL, NULL, dest, NULL);
            }

            PopActiveSnapshot();
        }

        // Clean up per-statement memory
        MemoryContextSwitchTo(oldcontext);
        MemoryContextDelete(per_stmt_context);
    }

    // Ensure final changes are visible
    CommandCounterIncrement();
}
```