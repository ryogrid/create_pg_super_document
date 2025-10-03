# get_json_object_as_hash

## Location
[src/backend/utils/adt/jsonfuncs.c:3809-3850](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3809-L3850)

## Overview
A static function that parses a JSON object string and decomposes it into a PostgreSQL hash table for efficient field access during record population operations.

## Definition

```c
static HTAB *
get_json_object_as_hash(const char *json, int len, const char *funcname,
						Node *escontext)
```
## Detailed Description
This function parses a JSON object and creates a hash table containing all the key-value pairs for efficient lookup during record population. It uses PostgreSQL's JSON parser with custom semantic actions to populate the hash table. The function sets up proper lexical context, configures semantic actions for JSON parsing events, and handles errors gracefully by cleaning up resources and returning NULL on parse failures.

Key behaviors:
- Creates a hash table with string keys (field names) and JsonHashEntry values
- Sets up JSON lexical context with proper encoding
- Configures semantic actions for JSON parsing events (arrays, scalars, object fields)
- Parses JSON using PostgreSQL's error-safe parser
- Cleans up resources and returns NULL on parse errors
- Uses current memory context for hash table allocation

## Parameters / Member Variables
- `*json`: Pointer to the JSON string to be parsed
- `len`: Length of the JSON string in bytes
- `*funcname`: Name of the calling function (used for error reporting context)
- `*escontext`: Error context for soft error handling during parsing
## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md)
  - [palloc0](../p/palloc0.md)
  - [makeJsonLexContextCstringLen](../m/makeJsonLexContextCstringLen.md)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - [pg_parse_json_or_errsave](../p/pg_parse_json_or_errsave.md)
  - [hash_destroy](../h/hash_destroy.md)
  - [freeJsonLexContext](../f/freeJsonLexContext.md)
  - [hash_array_start](../h/hash_array_start.md)
  - [hash_scalar](../h/hash_scalar.md)
  - [hash_object_field_start](../h/hash_object_field_start.md)
  - [hash_object_field_end](../h/hash_object_field_end.md)
  - HASH_ELEM
  - HASH_STRINGS
  - HASH_CONTEXT
- Called from (representative examples):
  - [JsValueToJsObject](../J/JsValueToJsObject.md)

## Notes and Other Information
- This is a static function used internally by JSON processing infrastructure
- Creates hash table with NAMEDATALEN key size to accommodate PostgreSQL identifier limits
- Uses semantic actions to handle different JSON elements during parsing
- Implements proper resource cleanup on parsing failures
- Part of the JSON object access optimization for record population
- The hash table enables O(1) field lookup instead of linear JSON traversal
- Uses PostgreSQL's standard hash table implementation with string keys
- Handles encoding properly through GetDatabaseEncoding()

## Simplified Source

```c
static HTAB *
get_json_object_as_hash(const char *json, int len, const char *funcname,
                        Node *escontext)
{
    // Create hash table for JSON field storage
    HASHCTL ctl;
    ctl.keysize = NAMEDATALEN;
    ctl.entrysize = sizeof(JsonHashEntry);
    ctl.hcxt = CurrentMemoryContext;

    HTAB *tab = hash_create("json object hashtable", 100, &ctl,
                           HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

    // Set up parsing state and semantic actions
    JHashState *state = palloc0(sizeof(JHashState));
    JsonSemAction *sem = palloc0(sizeof(JsonSemAction));

    state->function_name = funcname;
    state->hash = tab;
    state->lex = makeJsonLexContextCstringLen(NULL, json, len,
                                             GetDatabaseEncoding(), true);

    // Configure semantic actions for JSON events
    sem->semstate = (void *) state;
    sem->array_start = hash_array_start;
    sem->scalar = hash_scalar;
    sem->object_field_start = hash_object_field_start;
    sem->object_field_end = hash_object_field_end;

    // Parse JSON and handle errors
    if (!pg_parse_json_or_errsave(state->lex, sem, escontext)) {
        hash_destroy(state->hash);
        tab = NULL;
    }

    freeJsonLexContext(state->lex);
    return tab;
}
```