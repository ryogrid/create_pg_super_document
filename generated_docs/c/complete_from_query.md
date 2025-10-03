# complete_from_query

## Location
[src/bin/psql/tab-complete.c:5160-5167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5160-L5167)

## Overview
A tab completion generator function that provides autocompletion suggestions by executing SQL queries against the PostgreSQL server and returning matching results.

## Definition

```c
static char *
complete_from_query(const char *text, int state)
```
## Detailed Description
This function serves as a wrapper around the more general  function, providing a simplified interface for query-based completions. It executes SQL queries to retrieve completion candidates from the PostgreSQL server, making it possible to provide dynamic, context-aware suggestions based on actual database content (such as table names, column names, function names, etc.).

The function assumes the query will work for any server version and uses global completion variables to pass query parameters and configuration to the underlying completion engine. This allows psql to provide intelligent autocompletion that reflects the current state of the connected database.

## Parameters / Member Variables
- `*text`: The partial text that the user has typed, used to filter completion results
- `state`: Call counter maintained by readline - 0 for first call, incremented on subsequent calls
## Dependencies
- Functions called/Symbols referenced:
  - : Core function that handles query execution and result processing
- Called from (representative examples):
  - : Macro for query-based list completions
  - : Macro for verbatim query completions
  - : Timezone name completion
  - : Tab completion system integration

## Notes and Other Information
This function is part of psql's dynamic completion system that queries the database server to provide accurate, up-to-date completion suggestions. It uses global completion variables (, , ) that are set by various completion macros before calling this function. This design allows the same core query mechanism to be used for different types of completions while maintaining different query parameters and result processing options.