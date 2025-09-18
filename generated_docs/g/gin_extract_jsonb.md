# gin_extract_jsonb

## Location
src/backend/utils/adt/jsonb_gin.c: 229 - 277

## Overview
A GIN opclass support function that extracts indexable entries from a JSONB value by iterating through all keys, values, and array elements to create individual index entries.

## Definition


## Detailed Description
This function implements the extract operation for the jsonb_ops GIN operator class. It takes a JSONB value and extracts all indexable components (keys, values, and array elements) into separate GIN index entries. The function uses a JsonbIterator to traverse the entire JSONB structure, processing each encountered element based on its type:

- **Keys (WJB_KEY)**: Object keys are converted to index entries with key semantics
- **Values (WJB_VALUE)**: Object values are converted to index entries with value semantics  
- **Array Elements (WJB_ELEM)**: Array elements are treated as keys if they are strings, otherwise as values

The function uses an intelligent pre-allocation strategy, initially allocating space for 2 times the root count of elements, then dynamically expanding as needed. This extraction process enables efficient JSONB queries using GIN indexes, supporting operations like containment checks and key/value existence tests.

## Parameters / Member Variables
- : Standard PostgreSQL function interface accepting:
  - arg0: JSONB value to extract entries from (retrieved via PG_GETARG_JSONB_P(0))
  - arg1: Pointer to store the number of extracted entries (int32 *nentries)

## Return Value
- Returns a pointer to an array of Datum values representing the extracted index entries
- Sets *nentries to the count of extracted entries
- Returns NULL if no entries are found (empty JSONB)

## Dependencies
- Functions called/Symbols referenced:
  - [init_gin_entries](../i/init_gin_entries.md) (initializes entry buffer)
  - [add_gin_entry](../a/add_gin_entry.md) (adds entries to buffer)
  - [make_scalar_key](../m/make_scalar_key.md) (converts JsonbValue to indexable Datum)
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md) (initializes JSONB iterator)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md) (advances through JSONB structure)
  - JB_ROOT_COUNT (counts root-level elements)
- Called from (representative examples):
  - [gin_extract_jsonb_query](gin_extract_jsonb_query.md) (at src/backend/utils/adt/jsonb_gin.c:859)
  - System-level GIN index operations (via opclass registration)

## Notes and Other Information
- This function is part of the jsonb_ops GIN opclass infrastructure
- Uses efficient pre-allocation based on root element count to minimize memory reallocations
- String array elements are treated specially - they are indexed as keys rather than values to support array containment queries
- Structural JSONB tokens (like object/array start/end markers) are ignored during extraction
- The function signature follows PostgreSQL's V1 calling convention
- Memory management is handled by the GinEntries buffer system
- Critical for enabling JSONB containment operators (@>, <@) and key existence operators (?, ?&, ?|)