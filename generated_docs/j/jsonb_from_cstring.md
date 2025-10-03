## Simplified Source

```c
static inline Datum
jsonb_from_cstring(char *json, int len, bool unique_keys, Node *escontext)
{
    JsonLexContext lex;
    JsonbInState state;
    JsonSemAction sem;

    // Initialize parser state and semantic actions
    memset(&state, 0, sizeof(state));
    memset(&sem, 0, sizeof(sem));
    makeJsonLexContextCstringLen(&lex, json, len, GetDatabaseEncoding(), true);

    // Configure parsing state
    state.unique_keys = unique_keys;
    state.escontext = escontext;
    sem.semstate = (void *) &state;

    // Set up semantic action callbacks for JSON parsing
    sem.object_start = jsonb_in_object_start;
    sem.array_start = jsonb_in_array_start;
    sem.object_end = jsonb_in_object_end;
    sem.array_end = jsonb_in_array_end;
    sem.scalar = jsonb_in_scalar;
    sem.object_field_start = jsonb_in_object_field_start;

    // Parse JSON string into internal representation
    if (!pg_parse_json_or_errsave(&lex, &sem, escontext))
        return (Datum) 0;

    // Convert parsed result to final jsonb structure
    PG_RETURN_POINTER(JsonbValueToJsonb(state.res));
}
```