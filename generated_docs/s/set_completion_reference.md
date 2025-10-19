# set_completion_reference

## Location
[src/bin/psql/tab-complete.c:5597-5611](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5597-L5611)

## Overview
Sets up global reference variables used by completion queries to provide context-aware tab completion in PostgreSQL.

## Definition
```c
static void set_completion_reference(const char *word)
```

## Detailed Description
This function parses a given word (typically an SQL identifier like a table name or function name) into its schema and object components, storing the results in global variables completion_ref_schema and completion_ref_object. These reference variables are then used by subsequent completion queries executed through _complete_from_query to provide contextually appropriate completions. For example, when completing column names, the function would be called with a table name to set the reference, allowing the completion system to query for columns specific to that table.

The function uses parse_identifier to handle both qualified (schema.object) and unqualified identifiers, properly managing quoted identifiers and SQL naming conventions. The parsed components become available for substitution into query templates that include %s placeholders for reference objects and schemas.

## Parameters / Member Variables
- `word`: The SQL identifier to parse, which may be qualified (schema.object) or unqualified

## Dependencies
- Functions called/Symbols referenced:
  - [parse_identifier](../p/parse_identifier.md)
- Called from (representative examples):
  - COMPLETE_WITH_ATTR_LIST
  - COMPLETE_WITH_ENUM_VALUE
  - COMPLETE_WITH_FUNCTION_ARG
  - HeadMatchesCS (multiple instances)

## Notes and Other Information
- Sets global variables completion_ref_schema and completion_ref_object for use in subsequent queries
- The schemaquoted and objectquoted variables are local and used only during parsing
- Widely used throughout the completion system, with over 35 different call sites
- Essential for providing context-aware completions (e.g., columns for a specific table, arguments for a specific function)
- The function is static and relatively simple, serving as a utility for the broader completion system
- Works in conjunction with _complete_from_query which uses the reference values in query construction

## Simplified Source

```c
static void set_completion_reference(const char *word) {
    bool schemaquoted, objectquoted;

    // Parse identifier into schema and object components
    // Results stored in global completion_ref_schema and completion_ref_object
    parse_identifier(word,
                    &completion_ref_schema, &completion_ref_object,
                    &schemaquoted, &objectquoted);
}
```