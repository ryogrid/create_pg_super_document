# getJsonbLength

## Location
src/backend/utils/adt/jsonb_util.c: 159 - 190

## Overview
Determines the byte length of the variable-length data portion for a specific JSONB node within its container.

## Definition
```c
uint32 getJsonbLength(const JsonbContainer *jc, int index)
```

## Detailed Description
This function calculates the length of variable-length data for a JSONB node identified by its index within a JsonbContainer. The JSONB format uses two different encoding strategies for storing lengths in JEntry structures:

1. **Direct length storage**: For smaller values, the length is stored directly in the JEntry's offset/length field
2. **Offset-based length**: For entries that require absolute positioning, the JEntry stores an end offset, and the length is calculated by subtracting the starting offset from this end offset

The function automatically detects which encoding is used by checking the JBE_HAS_OFF flag and applies the appropriate calculation method.

## Parameters / Member Variables
- `jc`: Pointer to the JsonbContainer structure containing the JSONB data
- `index`: Zero-based index of the target entry within the container's JEntry array

## Dependencies
- Functions called/Symbols referenced:
  - JBE_HAS_OFF (macro to check if JEntry contains an offset)
  - getJsonbOffset (gets the starting offset of variable-length data)
  - JBE_OFFLENFLD (macro to extract offset/length field from JEntry)
  - JsonbContainer (structure type)
- Called from (representative examples):
  - getKeyJsonValueFromContainer
  - fillJsonbValue
  - PG_RETURN_JSONB_P

## Notes and Other Information
- Returns a uint32 value representing the length in bytes of the variable-length data
- Works in conjunction with getJsonbOffset to provide complete addressing information for JSONB nodes
- Handles both length-only and offset+length JEntry encoding automatically
- Essential for extracting variable-length data like strings and nested containers from JSONB
- Located in src/backend/utils/adt/jsonb_util.c:159-190