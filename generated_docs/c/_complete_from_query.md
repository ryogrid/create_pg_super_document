# _complete_from_query

## Location
[src/bin/psql/tab-complete.c:5250-5596](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5250-L5596)

## Overview
Core function that performs tab completion for PostgreSQL commands by executing database queries and processing results to generate completion candidates.

## Definition
```c
static char *_complete_from_query(const char *simple_query,
                                 const SchemaQuery *schema_query,
                                 const char *const *keywords,
                                 bool verbatim,
                                 const char *text, int state)
```

## Detailed Description
This is the workhorse function of psql's tab completion system. It accepts two types of queries: simple queries with LIKE patterns and complex schema queries for database objects. The function handles the complete lifecycle of tab completion including parsing user input, constructing appropriate SQL queries, executing them against the PostgreSQL server, and formatting results for readline. It supports both qualified (schema.object) and unqualified object names, handles proper SQL identifier quoting, implements system catalog filtering (suppressing pg_* objects unless explicitly requested), and provides keyword completion alongside query results.

Key features include:
- Parses partially-typed identifiers into schema and object components
- Constructs dynamic SQL queries with proper escaping and LIKE patterns
- Supports reference objects through completion_ref_object/completion_ref_schema
- Handles both verbatim and identifier-aware completion modes
- Implements intelligent quoting based on user input patterns
- Provides fallback to keyword completion when query results are exhausted

## Parameters / Member Variables
- `simple_query`: A sprintf-style format string for simple completion queries with %s placeholders
- `schema_query`: A SchemaQuery structure defining complex schema-aware completion behavior
- `keywords`: Null-terminated array of literal keywords to include in completion
- `verbatim`: If true, return matches as-is; if false, parse and quote SQL identifiers appropriately
- `text`: The partial text being completed (from readline)
- `state`: Completion state (0 for first call, non-zero for subsequent calls)

## Dependencies
- Functions called/Symbols referenced:
  - [SchemaQuery](../S/SchemaQuery.md) (struct type)
  - [PQExpBufferData](../P/PQExpBufferData.md) (struct type)
  - [parse_identifier](../p/parse_identifier.md)
  - [make_like_pattern](../m/make_like_pattern.md)
  - [escape_string](../e/escape_string.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [exec_query](../e/exec_query.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - PGRES_TUPLES_OK
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQnfields](../P/PQnfields.md)
  - [identifier_needs_quotes](../i/identifier_needs_quotes.md)
  - [requote_identifier](../r/requote_identifier.md)
  - [pg_strncasecmp](../p/pg_strncasecmp.md)
  - [pg_strdup_keyword_case](../p/pg_strdup_keyword_case.md)
- Called from (representative examples):
  - [complete_from_query](complete_from_query.md)
  - [complete_from_versioned_query](complete_from_versioned_query.md)
  - [complete_from_schema_query](complete_from_schema_query.md)
  - [complete_from_versioned_schema_query](complete_from_versioned_schema_query.md)

## Notes and Other Information
- Uses static variables to maintain state across multiple readline calls for the same completion
- Implements sophisticated logic for system catalog suppression (hides pg_* objects unless input starts with 'pg_')
- Supports up to three %s substitutions in simple queries: object pattern, reference object, reference schema
- Limits result sets using completion_max_records to prevent overwhelming the user
- Implements a hack to prevent readline from adding spaces after bare schema names
- Handles memory management carefully to prevent leaks across completion sessions
- The function is static and quite large (340+ lines), indicating its central importance to the completion system

## Simplified Source

```c
static char *_complete_from_query(const char *simple_query,
                                 const SchemaQuery *schema_query,
                                 const char *const *keywords,
                                 bool verbatim,
                                 const char *text, int state) {
    static int list_index, num_schema_only, num_query_other, num_keywords;
    static PGresult *result = NULL;
    static bool non_empty_object, schemaquoted, objectquoted;

    // Initialize on first call
    if (state == 0) {
        // Reset static state
        list_index = 0;
        num_schema_only = num_query_other = num_keywords = 0;
        PQclear(result);
        result = NULL;

        // Parse input text into schema and object parts
        char *schemaname, *objectname;
        if (verbatim) {
            objectname = pg_strdup(text);
            schemaname = NULL;
        } else {
            parse_identifier(text, &schemaname, &objectname,
                           &schemaquoted, &objectquoted);
        }

        non_empty_object = (*objectname != '\0');

        // Create LIKE pattern and escape strings for query
        char *e_object_like = make_like_pattern(objectname);
        char *e_schemaname = schemaname ? escape_string(schemaname) : NULL;
        char *e_ref_object = completion_ref_object ? escape_string(completion_ref_object) : NULL;
        char *e_ref_schema = completion_ref_schema ? escape_string(completion_ref_schema) : NULL;

        // Build query
        PQExpBufferData query_buffer;
        initPQExpBuffer(&query_buffer);

        if (schema_query) {
            // Complex schema-aware query construction
            if (schemaname == NULL || schema_query->namespace == NULL) {
                // Unqualified name completion
                appendPQExpBuffer(&query_buffer, "SELECT %s%s, NULL::pg_catalog.text FROM %s",
                                schema_query->use_distinct ? "DISTINCT " : "",
                                schema_query->result, schema_query->catname);
                // Add WHERE clause with conditions and LIKE pattern
                appendPQExpBuffer(&query_buffer, " WHERE (%s) LIKE '%s'",
                                schema_query->result, e_object_like);
                // Add schema completion if supported
                if (schema_query->namespace) {
                    appendPQExpBuffer(&query_buffer,
                                    "\nUNION ALL\nSELECT NULL::pg_catalog.text, n.nspname "
                                    "FROM pg_catalog.pg_namespace n WHERE n.nspname LIKE '%s'",
                                    e_object_like);
                }
            } else {
                // Qualified name completion
                appendPQExpBuffer(&query_buffer, "SELECT %s%s, n.nspname FROM %s, pg_catalog.pg_namespace n",
                                schema_query->use_distinct ? "DISTINCT " : "",
                                schema_query->result, schema_query->catname);
                appendPQExpBuffer(&query_buffer, " WHERE %s = n.oid AND (%s) LIKE '%s' AND n.nspname = '%s'",
                                schema_query->namespace, schema_query->result,
                                e_object_like, e_schemaname);
            }
        } else {
            // Simple query with sprintf-style formatting
            appendPQExpBuffer(&query_buffer, simple_query,
                            e_object_like, e_ref_object, e_ref_schema);
        }

        // Add result limit and execute query
        appendPQExpBuffer(&query_buffer, "\nLIMIT %d", completion_max_records);
        result = exec_query(query_buffer.data);

        // Cleanup
        termPQExpBuffer(&query_buffer);
        free(schemaname);
        free(objectname);
        free(e_object_like);
        free(e_schemaname);
        free(e_ref_object);
        free(e_ref_schema);
    }

    // Return next result from query
    if (result && PQresultStatus(result) == PGRES_TUPLES_OK) {
        while (list_index < PQntuples(result)) {
            const char *item = NULL, *nsp = NULL;

            if (!PQgetisnull(result, list_index, 0))
                item = PQgetvalue(result, list_index, 0);
            if (PQnfields(result) > 1 && !PQgetisnull(result, list_index, 1))
                nsp = PQgetvalue(result, list_index, 1);
            list_index++;

            // Return verbatim or check quoting requirements
            if (verbatim) {
                num_query_other++;
                return pg_strdup(item);
            }

            // Skip items requiring quotes if user input wasn't quoted
            if (non_empty_object) {
                if (item && !objectquoted && identifier_needs_quotes(item))
                    continue;
                if (nsp && !schemaquoted && identifier_needs_quotes(nsp))
                    continue;
            }

            // Count and return properly quoted result
            if (item == NULL && nsp != NULL)
                num_schema_only++;
            else
                num_query_other++;

            return requote_identifier(nsp, item, schemaquoted, objectquoted);
        }

        // Try keyword completion after query results exhausted
        int nskip = list_index - PQntuples(result);

        // Check schema query keywords
        if (schema_query && schema_query->keywords) {
            const char *const *itemp = schema_query->keywords;
            while (*itemp) {
                const char *item = *itemp++;
                if (nskip-- <= 0) {
                    list_index++;
                    if (pg_strncasecmp(text, item, strlen(text)) == 0) {
                        num_keywords++;
                        return pg_strdup_keyword_case(item, text);
                    }
                }
            }
        }

        // Check additional keywords
        if (keywords) {
            const char *const *itemp = keywords;
            while (*itemp) {
                const char *item = *itemp++;
                if (nskip-- <= 0) {
                    list_index++;
                    if (pg_strncasecmp(text, item, strlen(text)) == 0) {
                        num_keywords++;
                        return pg_strdup_keyword_case(item, text);
                    }
                }
            }
        }
    }

    // Completion finished - special handling for schema-only results
    if (num_schema_only > 0 && num_query_other == 0 && num_keywords == 0)
        rl_completion_append_character = '\0';

    // Cleanup and return NULL to indicate completion end
    PQclear(result);
    result = NULL;
    return NULL;
}
```