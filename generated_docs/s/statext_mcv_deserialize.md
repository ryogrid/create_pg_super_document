# statext_mcv_deserialize

## Location
[src/backend/statistics/mcv.c:996-1337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L996-L1337)

## Overview
Deserializes a binary representation of an MCV (Most Common Values) statistics list stored as a bytea into an in-memory MCVList structure, performing comprehensive validation and memory layout optimization.

## Definition


## Detailed Description
This function reconstructs an MCVList structure from its serialized binary form. The deserialization process involves:

1. **Header validation**: Verifies magic number, type, dimensions count, and items count
2. **Size validation**: Ensures the input data matches expected size calculations
3. **Memory layout optimization**: Allocates all required memory as a single contiguous chunk for efficient access and cleanup
4. **Value reconstruction**: Rebuilds individual MCV items with proper value mappings and null flags
5. **Type-specific handling**: Handles by-value types, fixed-length by-reference types, varlena types, and cstring types differently

The function creates a mapping array to translate serialized value indexes back to actual Datum values, ensuring proper alignment and memory management throughout the process.

## Parameters / Member Variables
- : Input bytea containing the serialized MCV list data. If NULL, the function returns NULL immediately.

## Dependencies
- Functions called/Symbols referenced:
  -  - Get size of variable-length data
  -  - Get data portion of variable-length structure  
  - // - Memory allocation functions
  -  - Attribute fetching utility
  - / - Varlena manipulation macros
  -  - Convert pointer to Datum
  - Constants: , , , 

- Called from (representative examples):
  -  - Loads MCV statistics from system catalogs
  -  - SQL function to expose MCV list contents

## Notes and Other Information
- Allocates all memory as a single chunk for efficient cleanup with a single pfree()
- Performs extensive validation to prevent corruption from malformed input data
- Handles different PostgreSQL data types (by-value, fixed-length by-reference, varlena, cstring) with type-specific deserialization logic
- Uses alignment-aware memory layout to ensure proper data structure alignment
- The deserialized structure maintains the same logical organization as the original MCV list but with optimized memory layout for runtime usage
- Critical for extended statistics functionality in PostgreSQL's query planner