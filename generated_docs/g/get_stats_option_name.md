# get_stats_option_name

## Location
[src/backend/tcop/postgres.c:3837-3876](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3837-L3876)

## Overview
A utility function that maps single-character command-line option arguments to their corresponding PostgreSQL statistics logging GUC parameter names.

## Definition

```c
const char *
get_stats_option_name(const char *arg)
```
## Detailed Description
This function serves as a mapping utility for PostgreSQL's statistics logging command-line options. It takes a command-line argument string and returns the corresponding GUC (Grand Unified Configuration) parameter name for statistics logging. The function specifically handles three types of statistics logging options:

- Parser statistics (triggered by 'pa' argument)
- Planner statistics (triggered by 'pl' argument)  
- Executor statistics (triggered by 'e' argument)

The function uses a simple switch statement on the first character of the argument, with additional character checking for disambiguation between parser and planner options.

## Parameters / Member Variables
- `*arg`: A string containing the command-line argument that specifies which statistics option to enable. Expected values are "pa" for parser, "pl" for planner, or "e" for executor.
## Dependencies
- Functions called/Symbols referenced:
  - No external function calls
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md) (in src/backend/postmaster/postmaster.c:726)
  - [process_postgres_switches](../p/process_postgres_switches.md) (in src/backend/tcop/postgres.c:4048)

## Notes and Other Information
- Returns NULL if the argument doesn't match any recognized statistics option
- The function assumes the argument string is at least one character long
- Part of PostgreSQL's command-line argument processing infrastructure
- Used during server startup to enable specific statistics logging based on command-line flags
- The returned string corresponds to actual GUC parameter names that can be set in postgresql.conf

## Simplified Source

```c
// Simplified version of get_stats_option_name
const char *
get_stats_option_name(const char *arg) {
    // Check first character to determine stats type
    switch (arg[0]) {
        case 'p':
            // Distinguish between parser and planner stats
            if (arg[1] == 'a')    // "parser"
                return "log_parser_stats";
            else if (arg[1] == 'l')  // "planner"
                return "log_planner_stats";
            break;

        case 'e':    // "executor"
            return "log_executor_stats";
            break;
    }

    // Return NULL for unrecognized options
    return NULL;
}
```

Key simplifications made:
- Added clear comments explaining the logic flow
- Simplified the switch case structure for better readability
- Made the two-character checking logic more explicit
- Added descriptive comments for each case
- Focused on the main mapping functionality