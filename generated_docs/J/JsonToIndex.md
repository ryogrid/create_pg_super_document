# JsonToIndex

## Location
src/include/utils/jsonfuncs.h: 32 - 46

## Overview
JsonToIndex is an enumeration type that defines bit flags used to specify which types of elements should be processed when iterating through JSON or JSONB documents.

## Definition


## Detailed Description
JsonToIndex provides a set of bit flags that control which elements from JSON or JSONB documents are processed during iteration operations. The enumeration follows a bit-flag pattern where each value represents a specific type of JSON element, allowing for flexible combinations of element types to be selected for processing.

The enum is primarily used by the  and  functions to filter which JSON elements trigger the specified action callback. This selective processing capability is essential for operations like text search indexing, where only certain types of values (e.g., string values) need to be extracted and processed.

The flags can be combined using bitwise OR operations to create custom combinations of element types for processing.

## Parameters / Member Variables
-  (0x01): Flag to include JSON object keys in processing
-  (0x02): Flag to include JSON string values in processing  
-  (0x04): Flag to include JSON numeric values in processing
-  (0x08): Flag to include JSON boolean values in processing
- : Convenience flag that combines all other flags (jtiKey | jtiString | jtiNumeric | jtiBool)

## Dependencies
- Functions that use JsonToIndex flags:
  - iterate_jsonb_values
  - iterate_json_values
  - parse_jsonb_index_flags
  - jsonb_to_tsvector_worker
  - json_to_tsvector_worker
- Used in contexts:
  - JSON/JSONB value iteration and processing
  - Text search vector generation from JSON documents
  - JSON element filtering operations

## Notes and Other Information
- The enum uses bit flag values (powers of 2) to allow combining multiple element types using bitwise OR operations
- Commonly used in PostgreSQL's full-text search functionality where  is frequently used to extract only string values for indexing
- The flags are passed as  parameters to iteration functions, allowing for future expansion of flag types
- Located in 
- The design allows for efficient filtering during JSON parsing without requiring multiple passes through the document