# execute_sql_string

## Location
src/backend/commands/extension.c: 741 - 847

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
  - pg_parse_query (parses SQL string into parse trees)
  - CreateDestReceiver (creates output destination)
  - AllocSetContextCreate (creates memory context)
  - CommandCounterIncrement (makes DDL changes visible)
  - pg_analyze_and_rewrite_fixedparams (analyzes and rewrites queries)
  - pg_plan_queries (creates execution plans)
  - CreateQueryDesc (creates query descriptor)
  - ExecutorStart/ExecutorRun/ExecutorFinish/ExecutorEnd (query execution)
  - ProcessUtility (executes utility statements)
  - PushActiveSnapshot/PopActiveSnapshot (snapshot management)
- Called from:
  - execute_extension_script

## Notes and Other Information
- This is a static function within the extension.c module
- Explicitly avoids using SPI due to limitations with forward references and error reporting
- Uses DestNone to discard SELECT output during script execution
- Prohibits transaction control statements (BEGIN, COMMIT, ROLLBACK) in extension scripts
- Memory management uses per-statement contexts to prevent excessive memory usage
- Essential for proper extension installation where scripts may contain interdependent DDL statements
- The sequential execution model ensures that each statement sees the effects of previous statements