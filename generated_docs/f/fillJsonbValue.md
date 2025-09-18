# fillJsonbValue

## Location
src/backend/utils/adt/jsonb_util.c: 502 - 562

## Overview
A low-level helper function that extracts and converts JSONB element data from internal storage format into a JsonbValue structure.

## Definition


## Detailed Description
This fundamental utility function decodes JSONB elements from their compact internal representation into the more accessible JsonbValue format. It examines the JEntry metadata to determine the data type and location, then appropriately sets up the JsonbValue fields based on the element's type.

The function handles all JSONB data types:
- Null values: Sets type to jbvNull
- Strings: Points to string data with length information
- Numeric values: Points to aligned numeric data  
- Booleans: Decodes true/false from entry flags
- Containers (arrays/objects): Returns as jbvBinary without expansion

For variable-length data, it relies on caller-provided offset calculations for performance optimization, allowing callers to amortize offset computation across multiple elements. The function handles memory alignment requirements, particularly for numeric data and nested containers.

## Parameters / Member Variables
- : The JSONB container holding the element
- : Index of the element within the container's JEntry array
- : Base address where variable-length element data begins
- : Byte offset from base_addr to this element's data
- : Pre-allocated JsonbValue structure to fill with element data

## Dependencies
- Functions called/Symbols referenced:
  - getJsonbLength
  - JBE_ISNULL, JBE_ISSTRING, JBE_ISNUMERIC, JBE_ISBOOL_TRUE, JBE_ISBOOL_FALSE, JBE_ISCONTAINER (macros)
  - INTALIGN (alignment macro)
  - jbvNull, jbvString, jbvNumeric, jbvBool, jbvBinary (enum values)
- Called from (representative examples):
  - findJsonbValueFromContainer
  - getKeyJsonValueFromContainer
  - getIthJsonbValueFromContainer
  - JsonbIteratorNext

## Notes and Other Information
- Static function: internal to jsonb_util.c, not exposed in headers
- Performance optimization: caller provides offset to avoid repeated calculations
- Memory alignment: handles INTALIGN requirements for numeric and container data
- Nested containers are returned as unexpanded jbvBinary for efficient lazy evaluation
- No memory allocation: fills caller-provided JsonbValue structure
- Type safety: asserts string length is non-negative
- Pointer-based access: strings and containers reference data in-place rather than copying