# JsonbIteratorInit

## Location
src/backend/utils/adt/jsonb_util.c: 814 - 851

## Overview
Initializes a JsonbIterator for traversing and iterating over the elements of a JsonbContainer, expanding all items to their full in-memory representation for manipulation.

## Definition


## Detailed Description
JsonbIteratorInit serves as the entry point for creating an iterator over a JsonbContainer structure. It provides a simple wrapper around the internal iteratorFromContainer function, passing NULL as the parent parameter to create a root-level iterator. This function is fundamental to JSONB processing in PostgreSQL, enabling traversal of complex nested JSON structures including objects and arrays.

The iterator created by this function allows sequential access to all elements within the container, handling the complexity of nested structures internally. The iterator maintains state information necessary for depth-first traversal and proper memory management during iteration.

## Parameters / Member Variables
- : Pointer to the JsonbContainer structure to be iterated over

## Dependencies
- Functions called/Symbols referenced:
  - iteratorFromContainer
  - JsonbContainer
  - JsonbIteratorToken
- Called from (representative examples):
  - JsonbToCStringWorker
  - jsonb_contains
  - JsonbDeepContains
  - jsonb_object_keys
  - jsonb_set_element
  - each_worker_jsonb
  - elements_worker_jsonb

## Notes and Other Information
- This function is a thin wrapper around iteratorFromContainer with NULL parent parameter
- Memory management details are handled in JsonbIteratorNext() - see that function for memory management notes
- The iterator supports depth-first traversal of nested JSON structures
- Used extensively throughout PostgreSQL's JSONB implementation for various operations including containment checks, serialization, aggregation, and manipulation functions
- The function is the standard way to begin iteration over any JsonbContainer in PostgreSQL