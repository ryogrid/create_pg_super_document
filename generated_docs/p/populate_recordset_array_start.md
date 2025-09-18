# populate_recordset_array_start

## Location
[src/backend/utils/adt/jsonfuncs.c:4281-4287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4281-L4287)

## Overview
A static function that handles the start of a JSON array during JSON recordset population, serving as a no-operation callback in the JSON parsing framework.

## Definition
```c
static JsonParseErrorType populate_recordset_array_start(void *state)
```

## Detailed Description
This function is a callback handler for JSON parsing that is invoked when a JSON array opening bracket is encountered during recordset population. Unlike other callback functions in the recordset population system, this function performs no operations and immediately returns JSON_SUCCESS.

The function exists primarily to satisfy the JSON parser callback interface requirements. Since recordset population logic is handled at the array element level (by populate_recordset_array_element_start) and object level (by populate_recordset_object_start/end), no special processing is needed when an array begins.

This design allows the JSON parser to maintain a consistent callback interface while delegating the actual recordset processing logic to more specific callback functions that handle individual elements and objects within the array.

## Parameters / Member Variables
- `state`: A void pointer that would typically be cast to PopulateRecordsetState, but is not used in this function

## Dependencies
- Functions called/Symbols referenced:
  - JSON_SUCCESS (return value constant)
  - JsonParseErrorType (return type)
- Called from (representative examples):
  - [populate_recordset_worker](populate_recordset_worker.md)
  - JsObjectFree

## Notes and Other Information
- This function is part of the JSON recordset population infrastructure in PostgreSQL
- The function serves as a placeholder callback to maintain interface consistency
- No state management or validation is performed at the array start level
- The actual processing logic for recordset population occurs in the element-level and object-level callbacks
- This pattern is common in event-driven parsing systems where not all events require special handling