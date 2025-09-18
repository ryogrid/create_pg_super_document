# populate_recordset_record

## Location
src/backend/utils/adt/jsonfuncs.c: 4000 - 4038

## Overview
A static function that processes a single JSON object and converts it into a tuple record, storing it in the recordset state`s tuple store.

## Definition
```c
static void populate_recordset_record(PopulateRecordsetState *state, JsObject *obj)
```

## Detailed Description
The `populate_recordset_record` function is responsible for converting a single JSON object into a PostgreSQL tuple and storing it in the tuple store. It updates the cached tuple descriptor, populates the record from the JSON object using the `populate_record` function, performs domain constraint checking if needed, and finally stores the resulting tuple in the tuple store. This function is a key component in the JSON-to-recordset conversion process.

## Parameters / Member Variables
- `state`: Pointer to PopulateRecordsetState containing the conversion context and tuple store
- `obj`: Pointer to JsObject representing the JSON object to be converted into a record

## Dependencies
- Functions called/Symbols referenced:
  - [update_cached_tupdesc](../u/update_cached_tupdesc.md)
  - [populate_record](populate_record.md)
  - domain_check_safe
  - [HeapTupleHeaderGetDatum](../H/HeapTupleHeaderGetDatum.md)
  - HeapTupleHeaderGetDatumLength
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - tuplestore_puttuple
- Called from (representative examples):
  - [populate_recordset_worker](populate_recordset_worker.md)
  - [populate_recordset_object_end](populate_recordset_object_end.md)

## Notes and Other Information
- Located at src/backend/utils/adt/jsonfuncs.c:4000-4038
- Static function, only accessible within the same compilation unit
- Handles domain constraint checking for composite domain types using TYPECAT_COMPOSITE_DOMAIN
- Creates a HeapTupleData structure with proper metadata before storing in the tuple store
- Critical component in the pipeline for converting JSON arrays to PostgreSQL recordsets
- Memory management handled through the function`s memory context