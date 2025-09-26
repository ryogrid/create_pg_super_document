# get_utility_query_def

## Location
[src/backend/utils/adt/ruleutils.c:7285-7329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L7285-L7329)

## Overview
Generates the text representation of utility SQL statements from a parsed Query structure, currently supporting only NOTIFY commands within rule contexts.

## Definition
```c
static void get_utility_query_def(Query *query, deparse_context *context)
```

## Detailed Description
This function is responsible for deparsing (converting back to text) utility statements from PostgreSQL's internal Query representation. Unlike DML statements (SELECT, INSERT, UPDATE, DELETE, MERGE), utility statements represent administrative and control commands.

Currently, the function has limited scope and only handles NOTIFY statements, which are used for inter-process communication in PostgreSQL. The NOTIFY command allows sessions to send notifications to other sessions that are listening on a specific channel.

The function processes NOTIFY statements by:
1. Extracting the NotifyStmt from the query's utilityStmt field
2. Formatting the channel name using proper identifier quoting
3. Including the optional payload string with appropriate literal quoting
4. Generating output in the format: `NOTIFY channel_name [, 'payload']`

For any other utility statement type, the function raises an error, as only NOTIFY commands are expected to appear in rule definitions where this deparsing function is typically used.

## Parameters / Member Variables
- `query`: The Query structure containing the parsed utility statement to be deparsed
- `context`: The deparse_context containing formatting preferences, indentation level, and the output StringInfo buffer

## Dependencies
- Functions called/Symbols referenced:
  - [appendContextKeyword](../a/appendContextKeyword.md)
  - [quote_identifier](../q/quote_identifier.md)
  - [simple_quote_literal](../s/simple_quote_literal.md)
- Called from:
  - [get_query_def](get_query_def.md)

## Notes and Other Information
- This is a static function within ruleutils.c, part of PostgreSQL's rule decompilation system
- The function has very limited scope compared to other query deparsing functions, supporting only NOTIFY statements
- NOTIFY statements are the only utility commands that can legitimately appear in PostgreSQL rule definitions
- The function properly handles identifier quoting for channel names and literal quoting for payload strings
- Part of the broader query deparsing infrastructure, but with much more restricted functionality than DML statement deparsers
- The error case indicates that expanding support for other utility statements would require additional implementation
- In PostgreSQL's architecture, most utility statements are not stored in rules and therefore don't need deparsing support