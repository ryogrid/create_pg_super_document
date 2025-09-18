# JsObject

## Location
src/backend/utils/adt/jsonfuncs.c: 307 - 315

## Overview
JsObject is a structure that represents JSON objects in PostgreSQL, providing efficient access to key-value pairs for both JSON text and binary JSONB representations.

## Definition


## Detailed Description
JsObject provides a unified interface for working with JSON objects regardless of their underlying storage format (text JSON or binary JSONB). It uses a discriminated union pattern similar to JsValue, where the  flag determines the representation type.

For JSON text objects, it maintains a hash table (HTAB) that maps field names to their values for efficient key-based lookups. For JSONB objects, it holds a pointer to a JsonbContainer that provides direct access to the binary object structure.

This abstraction allows PostgreSQL's object manipulation functions to work uniformly with both JSON formats, enabling efficient field access, iteration, and modification operations.

## Parameters / Member Variables
- : Boolean flag indicating whether this contains a JSON text object (true) or JSONB binary object (false)
- : Hash table pointer used for JSON text objects, mapping field names to values for fast lookup
- : Pointer to JsonbContainer structure for binary JSONB objects, providing direct container access

## Dependencies
- Functions called/Symbols referenced:
  - HTAB (PostgreSQL hash table structure)
  - JsonbContainer (JSONB container structure)
- Called from (representative examples):
  - JsObjectFree (memory cleanup and deallocation)
  - JsValueToJsObject (conversion from JsValue to JsObject)
  - populate_composite (composite type population)
  - JsObjectGetField (field value retrieval)
  - populate_record (record population from JSON object)
  - populate_recordset_object_end (recordset processing)

## Notes and Other Information
- Essential for efficient JSON object field access in PostgreSQL
- Hash table implementation for JSON text provides O(1) average case field lookup
- JsonbContainer for JSONB provides direct binary access without parsing overhead
- Memory management differs between variants - hash tables require explicit cleanup while JSONB containers are managed differently
- Used extensively in record population, composite type handling, and JSON object manipulation
- Part of the internal JSON object processing infrastructure in PostgreSQL