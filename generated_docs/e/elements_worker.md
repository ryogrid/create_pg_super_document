# elements_worker

## Location
[src/backend/utils/adt/jsonfuncs.c:2306-2347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2306-L2347)

## Overview
A static worker function that parses JSON text and extracts array elements using PostgreSQL's JSON parsing framework with semantic actions.

## Definition

```c
static Datum
elements_worker(FunctionCallInfo fcinfo, const char *funcname, bool as_text)
```
## Detailed Description
This function serves as the core implementation for JSON array element extraction from text-based JSON (as opposed to binary JSONB). It uses PostgreSQL's JSON parsing infrastructure with semantic action callbacks to process JSON arrays. The function sets up a parsing context with specific semantic actions for handling array elements, then delegates to the JSON parser. Unlike elements_worker_jsonb which works with pre-parsed JSONB data, this function parses raw JSON text and uses callback-driven processing to extract array elements during the parsing phase.

## Parameters / Member Variables
- `fcinfo`: Function call information context containing arguments and result information
- `*funcname`: Name of the calling function (used for error reporting and state tracking)
- `as_text`: Boolean flag determining output format - true normalizes results to text, false keeps original JSON format
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP: Extract text argument from function call
  - [makeJsonLexContext](../m/makeJsonLexContext.md)/freeJsonLexContext: JSON lexical context management
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md): Initialize set-returning function framework
  - [elements_object_start](elements_object_start.md)/elements_scalar/elements_array_element_start/elements_array_element_end: Semantic action callback functions
  - pg_parse_json_or_ereport: Main JSON parsing function with error handling
  - AllocSetContextCreate/MemoryContextDelete: Memory management
- Called from:
  - [json_array_elements](../j/json_array_elements.md): Main entry point for JSON array element extraction
  - [json_array_elements_text](../j/json_array_elements_text.md): Main entry point for text-mode array element extraction

## Notes and Other Information
- Uses callback-driven JSON parsing with semantic actions rather than iterating through pre-parsed structures
- Creates temporary memory context for efficient cleanup during processing
- The ElementsState structure tracks parsing state and configuration across semantic action calls
- Only processes escaped strings when as_text is true, optimizing parsing for JSON output mode
- Part of PostgreSQL's text-based JSON processing pipeline, complementing the JSONB-based functions
- Error handling is delegated to pg_parse_json_or_ereport which provides detailed parse error reporting

## Simplified Source

```c
static Datum
elements_worker(FunctionCallInfo fcinfo, const char *funcname, bool as_text)
{
    text *json = PG_GETARG_TEXT_PP(0);
    JsonLexContext lex;
    JsonSemAction *sem;
    ReturnSetInfo *rsi;
    ElementsState *state;

    // Setup JSON lexical context (only escape strings when needed for text output)
    makeJsonLexContext(&lex, json, as_text);

    // Allocate state and semantic action structures
    state = palloc0(sizeof(ElementsState));
    sem = palloc0(sizeof(JsonSemAction));

    // Initialize set-returning function
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);
    rsi = (ReturnSetInfo *) fcinfo->resultinfo;
    state->tuple_store = rsi->setResult;
    state->ret_tdesc = rsi->setDesc;

    // Configure semantic action callbacks for JSON parsing
    sem->semstate = (void *) state;
    sem->object_start = elements_object_start;
    sem->scalar = elements_scalar;
    sem->array_element_start = elements_array_element_start;
    sem->array_element_end = elements_array_element_end;

    // Initialize state for element extraction
    state->function_name = funcname;
    state->normalize_results = as_text;
    state->next_scalar = false;
    state->lex = &lex;
    state->tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                           "json_array_elements temporary cxt",
                                           ALLOCSET_DEFAULT_SIZES);

    // Parse JSON with semantic actions to extract elements
    pg_parse_json_or_ereport(&lex, sem);

    // Cleanup
    MemoryContextDelete(state->tmp_cxt);
    freeJsonLexContext(&lex);

    PG_RETURN_NULL();
}
```