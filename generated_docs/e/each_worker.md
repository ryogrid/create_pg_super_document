# each_worker

## Location
src/backend/utils/adt/jsonfuncs.c: 2056 - 2095

## Overview
The each_worker function is the core implementation for expanding JSON (text-based) objects into key-value pairs using PostgreSQL's JSON parsing infrastructure.

## Definition
```c
static Datum each_worker(FunctionCallInfo fcinfo, bool as_text)
```

## Detailed Description
This function implements the core logic for JSON object expansion operations on text-based JSON input. It sets up a JSON parsing context using JsonSemAction callbacks to handle different JSON elements during parsing. The function configures semantic actions for array start, scalar values, object field start, and object field end events. It uses a temporary memory context for efficient memory management and relies on PostgreSQL's JSON parser (pg_parse_json_or_ereport) to traverse the JSON structure. The parsed key-value pairs are stored in a tuple store for return as a set-returning function result.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing arguments and result context
- `as_text`: Boolean flag indicating whether values should be normalized to text format (true) or kept as JSON (false)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (get text argument)
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md) (initialize set-returning function)
  - [makeJsonLexContext](../m/makeJsonLexContext.md) (create JSON lexical context)
  - pg_parse_json_or_ereport (parse JSON with error handling)
  - [each_array_start](each_array_start.md), each_scalar, each_object_field_start, each_object_field_end (callback functions)
  - AllocSetContextCreate, MemoryContextDelete (memory management)
  - freeJsonLexContext (cleanup lexical context)
- Called from (representative examples):
  - [json_each](../j/json_each.md) (with as_text=false)
  - [json_each_text](../j/json_each_text.md) (with as_text=true)

## Notes and Other Information
- Located at src/backend/utils/adt/jsonfuncs.c:2056-2095
- Static function (internal implementation detail)
- Uses callback-based JSON parsing approach with JsonSemAction
- Implements proper memory management with temporary contexts
- Works with text-based JSON input (unlike each_worker_jsonb which works with binary JSONB)
- Uses EachState structure to maintain parsing state across callbacks
- Part of PostgreSQL's JSON (text) processing functionality
- Complements the JSONB-based each_worker_jsonb function