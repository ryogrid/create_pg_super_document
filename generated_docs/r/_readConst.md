# _readConst

## Location
src/backend/nodes/readfuncs.c: 259 - 280

## Overview
A static function that deserializes Const nodes from their textual representation, handling constant values used in PostgreSQL expression trees including their type information, nullability, and actual data values.

## Definition
```c
static Const *_readConst(void)
```

## Detailed Description
The `_readConst` function is responsible for deserializing Const nodes, which represent constant values in PostgreSQL's expression trees and query plans. Const nodes are fundamental building blocks that store literal values along with their complete type metadata.

The function follows PostgreSQL's standard node reading protocol:

1. **Metadata deserialization**: Reads type information (`consttype`, `consttypmod`, `constcollid`), storage details (`constlen`, `constbyval`), and location data
2. **Null handling**: Checks the `constisnull` flag to determine if the constant represents a NULL value
3. **Value processing**: For non-NULL values, calls `readDatum()` to deserialize the actual constant value using the `constbyval` flag to determine the appropriate reading strategy
4. **NULL values**: For NULL constants, simply skips over the placeholder "<>" token

This function supports PostgreSQL's comprehensive type system by preserving all necessary type information required for proper value interpretation and manipulation during query execution.

## Parameters / Member Variables
- No parameters (uses standard node reading context via `READ_LOCALS(Const)`)
- Returns: `Const *` - pointer to the deserialized Const node

## Dependencies
- Functions called/Symbols referenced:
  - `READ_LOCALS` (macro for local node reading setup)
  - `READ_OID_FIELD`, `READ_INT_FIELD`, `READ_BOOL_FIELD`, `READ_LOCATION_FIELD` (field reading macros)
  - `[pg_strtok](../p/pg_strtok.md)` (tokenization function)
  - `[readDatum](readDatum.md)` (for deserializing the actual constant value)
  - `READ_DONE` (macro for node reading completion)
- Called from (representative examples):
  - Used internally by the node reading system for Const node deserialization
  - Typically invoked through the node reading dispatch mechanism

## Notes and Other Information
- This is a static function with special handling for nodes with custom_read_write attributes
- Part of the specialized node reading functions that handle complex data structures requiring custom serialization logic
- The function carefully handles both by-value and by-reference data types through the `constbyval` flag
- Works in conjunction with `_outConst` in outfuncs.c for round-trip serialization
- Critical for query plan deserialization, allowing stored plans and distributed query processing
- The `:constvalue` token is explicitly skipped as it serves as a field marker in the serialized format