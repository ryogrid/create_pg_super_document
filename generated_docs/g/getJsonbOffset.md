# getJsonbOffset

## Location
src/backend/utils/adt/jsonb_util.c: 134 - 158

## Overview
Calculates the byte offset of a variable-length data portion for a specific JSONB node within its container's variable-length data section.

## Definition
```c
uint32 getJsonbOffset(const JsonbContainer *jc, int index)
```

## Detailed Description
This function computes the starting offset of variable-length data for a JSONB node identified by its index within a JsonbContainer. The JSONB format uses a compact binary representation where variable-length data (strings, nested containers, etc.) is stored separately from the fixed-size JEntry headers. Each JEntry contains either a length field or an offset+length field depending on its position.

The algorithm works by walking backwards from the target index to find the most recent JEntry that stores an absolute offset, then accumulating the lengths of all intervening entries to calculate the final offset. This approach leverages the JSONB storage optimization where not all entries need to store absolute offsets.

## Parameters / Member Variables
- `jc`: Pointer to the JsonbContainer structure containing the JSONB data
- `index`: Zero-based index of the target entry within the container's JEntry array

## Dependencies
- Functions called/Symbols referenced:
  - JBE_OFFLENFLD (macro to extract offset/length field from JEntry)
  - JBE_HAS_OFF (macro to check if JEntry contains an offset)
  - [JsonbContainer](../J/JsonbContainer.md) (structure type)
- Called from (representative examples):
  - [getJsonbLength](getJsonbLength.md)
  - [getKeyJsonValueFromContainer](getKeyJsonValueFromContainer.md)
  - [getIthJsonbValueFromContainer](getIthJsonbValueFromContainer.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - PG_RETURN_JSONB_P

## Notes and Other Information
- Returns a uint32 offset value representing bytes from the start of the variable-length data section
- The function assumes the index is valid and within bounds of the container's JEntry array
- Part of the low-level JSONB binary format handling infrastructure
- Essential for efficiently accessing variable-length data without parsing the entire container
- Located in src/backend/utils/adt/jsonb_util.c:134-158