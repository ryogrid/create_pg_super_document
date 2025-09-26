# JsonUniqueParsingState

## Location
src/backend/utils/adt/json.c: 57 - 64

## Overview
JsonUniqueParsingState is a comprehensive context structure that manages all state information required for JSON key uniqueness validation during JSON parsing operations.

## Definition

```c
typedef struct JsonUniqueParsingState
{
	JsonLexContext *lex;
	JsonUniqueCheckState check;
	JsonUniqueStackEntry *stack;
	int			id_counter;
	bool		unique;
} JsonUniqueParsingState;
```
## Detailed Description
JsonUniqueParsingState serves as the central coordination structure for PostgreSQL's JSON key uniqueness checking system during parsing operations. It integrates lexical analysis, hash-based key tracking, hierarchical scope management, and validation state into a single cohesive unit. This structure enables comprehensive duplicate key detection across nested JSON objects while maintaining efficient O(1) key lookup performance.

The structure coordinates between the JSON lexer for token processing, the hash table for key storage, the stack for nested object tracking, and maintains both sequential object identification and overall validation status. This design provides a complete solution for ensuring JSON objects comply with uniqueness requirements at all nesting levels.

## Parameters / Member Variables
- : Pointer to the JSON lexical analyzer context (JsonLexContext) that provides tokenized JSON input
- : Hash table state (JsonUniqueCheckState) for fast key lookup and duplicate detection
- : Pointer to the current top of the object scope stack (JsonUniqueStackEntry) for nested object tracking
- : Sequential counter for assigning unique object IDs to different JSON object scopes
- : Boolean flag indicating whether all keys processed so far are unique (true) or duplicates have been found (false)

## Dependencies
- Functions called/Symbols referenced:
  - JsonLexContext (JSON lexical analyzer)
  - JsonUniqueCheckState (hash table for key checking)
  - JsonUniqueStackEntry (stack for nested object tracking)
- Called from (representative examples):
  - json_unique_object_start (when entering new JSON objects)
  - json_unique_object_end (when exiting JSON objects)
  - json_unique_object_field_start (when processing object fields)
  - json_validate (main JSON validation entry point)

## Notes and Other Information
- Central control structure that coordinates all aspects of JSON key uniqueness validation
- Maintains state across the entire JSON parsing process, from start to completion
- The unique flag provides early termination capability - processing can stop as soon as the first duplicate is detected
- The id_counter ensures each nested object gets a unique identifier for proper scope separation
- Integrates seamlessly with PostgreSQL's JSON parsing infrastructure
- Designed for both validation-only operations and parsing with concurrent processing
- Memory management for the stack and hash table is coordinated through this structure
- Essential component for maintaining JSON specification compliance in PostgreSQL's JSON implementation