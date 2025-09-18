# PopulateRecordsetState

## Location
src/backend/utils/adt/jsonfuncs.c: 244 - 255

## Overview
A per-call state structure used by PostgreSQL's populate_recordset function to maintain context and intermediate data during JSON recordset population operations.

## Definition
```c
typedef struct PopulateRecordsetState
{
    JsonLexContext *lex;
    const char *function_name;
    HTAB       *json_hash;
    char       *saved_scalar;
    const char *save_json_start;
    JsonTokenType saved_token_type;
    Tuplestorestate *tuple_store;
    HeapTupleHeader rec;
    PopulateRecordCache *cache;
} PopulateRecordsetState;
```

## Detailed Description
PopulateRecordsetState serves as the central state management structure for populate_recordset operations in PostgreSQL. It maintains all necessary context information during the parsing and population of JSON arrays into PostgreSQL recordsets. The structure coordinates between JSON lexical analysis, tuple storage, and record caching to efficiently convert JSON array data into a set of PostgreSQL composite type records.

This state structure is essential for handling complex JSON-to-recordset transformations, managing memory allocation, maintaining parsing state, and coordinating with PostgreSQL's tuple storage mechanisms. It works closely with the JSON parsing infrastructure and PostgreSQL's type system to ensure accurate and efficient data conversion.

## Parameters / Member Variables
- `lex`: Pointer to JsonLexContext for JSON lexical analysis and parsing state management
- `function_name`: Name of the calling function, used for error reporting and context identification
- `json_hash`: Hash table (HTAB) for storing and organizing JSON key-value pairs during processing
- `saved_scalar`: Temporarily saved scalar value from JSON parsing for later processing
- `save_json_start`: Pointer to the start of the current JSON segment being processed
- `saved_token_type`: Type of the currently saved JSON token (JsonTokenType)
- `tuple_store`: Tuple store state for accumulating and managing the resulting recordset
- `rec`: Header of the current heap tuple being constructed
- `cache`: Pointer to PopulateRecordCache for metadata caching and performance optimization

## Dependencies
- Functions called/Symbols referenced:
  - JsonLexContext (JSON parsing context)
  - HTAB (hash table for JSON data storage)
  - JsonTokenType (enumeration for JSON token types)
  - Tuplestorestate (tuple storage management)
  - HeapTupleHeader (tuple header structure)
  - PopulateRecordCache (metadata caching structure)
- Called from (representative examples):
  - JsObjectFree (for cleanup operations)
  - populate_recordset_record
  - populate_recordset_worker
  - populate_recordset_object_start
  - populate_recordset_object_end
  - populate_recordset_array_element_start
  - populate_recordset_scalar
  - populate_recordset_object_field_start
  - populate_recordset_object_field_end

## Notes and Other Information
- This structure is allocated per function call and maintains state throughout the entire recordset population operation
- Essential for coordinating between JSON parsing and PostgreSQL's tuple storage system
- Used extensively in JSON processing callback functions to maintain consistency across parsing events
- The structure enables efficient processing of large JSON arrays by maintaining parsing state and reusing cached metadata
- Memory management is handled through PostgreSQL's memory context system