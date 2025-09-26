# elog_node_display

## Location
[src/backend/nodes/print.c:72-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/print.c#L72-L96)

## Overview
A utility function that sends formatted PostgreSQL Node contents to the postmaster log with configurable formatting and logging levels.

## Definition
```c
void elog_node_display(int lev, const char *title, const void *obj, bool pretty)
```

## Detailed Description
The `elog_node_display` function is a logging utility that outputs PostgreSQL Node structures to the server log rather than stdout. It provides flexible formatting options and integrates with PostgreSQL's error reporting system. The function converts the node to a string representation, applies either pretty formatting or standard formatting based on the `pretty` parameter, and logs it using the `ereport` mechanism with a specified logging level and title.

## Parameters / Member Variables
- `lev`: The logging level (e.g., DEBUG1, DEBUG2, LOG, NOTICE, WARNING, ERROR)
- `title`: A descriptive title that will appear in the log entry to identify the node being displayed
- `obj`: A pointer to the Node object to be logged (can be any PostgreSQL node type)
- `pretty`: Boolean flag determining whether to use pretty formatting (true) or standard formatting (false)

## Dependencies
- Functions called/Symbols referenced:
  - nodeToStringWithLocations: Converts the node to a string representation with location information
  - pretty_format_node_dump: Applies pretty formatting when pretty=true
  - format_node_dump: Applies standard formatting when pretty=false
  - ereport: PostgreSQL's error/log reporting function
  - errmsg_internal: Creates the main log message with the title
  - errdetail_internal: Adds the formatted node content as detail
  - pfree: Frees allocated memory

- Called from (representative examples):
  - pg_rewrite_query: Used during query rewriting phase for debugging
  - pg_plan_query: Used during query planning phase for debugging
  - nodeDisplay: Header declaration and macro usage

## Notes and Other Information
- This function integrates with PostgreSQL's logging infrastructure
- The output goes to the server log files, not stdout
- The logging level determines whether the message actually appears based on log settings
- Memory is properly managed with pfree calls to prevent leaks
- Commonly used for debugging query processing phases
- The title parameter helps identify different debugging contexts in log files
- Can be controlled by PostgreSQL's logging configuration parameters
- Located in src/backend/nodes/print.c:72-96