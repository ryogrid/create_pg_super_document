# each_worker_jsonb

## Location
[src/backend/utils/adt/jsonfuncs.c:1972-2055](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1972-L2055)

## Overview
The each_worker_jsonb function is the core implementation for expanding JSONB objects into key-value pairs, supporting both native JSONB and text output formats.

## Definition
```c
static Datum each_worker_jsonb(FunctionCallInfo fcinfo, const char *funcname, bool as_text)
```

## Detailed Description
This function implements the core logic for JSONB object expansion operations. It takes a JSONB object and iterates through its key-value pairs, returning them as a set of tuples. The function validates that the input is a JSONB object (not an array or scalar), then uses the JsonbIterator interface to traverse the object structure. For each key-value pair, it creates a tuple containing the key as text and the value in either JSONB format or text format depending on the as_text parameter. The function uses a temporary memory context for efficient memory management during iteration.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing arguments and result context
- `funcname`: String name of the calling function (used for error messages)
- `as_text`: Boolean flag indicating whether values should be returned as text (true) or native JSONB (false)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P (get JSONB argument)
  - JB_ROOT_IS_OBJECT (validate object type)
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md) (initialize set-returning function)
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md), JsonbIteratorNext (JSONB iteration)
  - cstring_to_text_with_len (key conversion)
  - [JsonbValueAsText](../J/JsonbValueAsText.md), JsonbValueToJsonb (value conversion)
  - tuplestore_putvalues (result storage)
  - AllocSetContextCreate, MemoryContextDelete (memory management)
- Called from (representative examples):
  - [jsonb_each](../j/jsonb_each.md) (with as_text=false)
  - [jsonb_each_text](../j/jsonb_each_text.md) (with as_text=true)

## Notes and Other Information
- Located at src/backend/utils/adt/jsonfuncs.c:1972-2055
- Static function (internal implementation detail)
- Uses MaterializedSRF pattern for set-returning functions
- Implements proper memory management with temporary contexts
- Handles JSON null values appropriately in text mode (converts to SQL NULL)
- Validates input type and reports meaningful error messages
- Core component of PostgreSQL's JSONB expansion functionality