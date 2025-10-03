# get_dtype

## Location
[src/interfaces/ecpg/preproc/type.c:693-748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.c#L693-L748)

## Overview
get_dtype is a utility function that converts ECPGdtype enumeration values to their corresponding string representations for code generation and debugging purposes.

## Definition

```c
const char *
get_dtype(enum ECPGdtype type)
```
## Detailed Description
This function serves as a lookup table that maps ECPGdtype enumeration values to their string equivalents. It's primarily used in ECPG code generation where descriptor type information needs to be output as literal strings in the generated C code. The function handles all defined ECPGdtype values including data descriptors, indicator information, precision/scale attributes, length information, and metadata descriptors. It provides error handling for unrecognized descriptor types through the mmerror reporting system.

## Parameters / Member Variables
- `type`: An ECPGdtype enumeration value representing a specific descriptor type that needs string conversion
## Dependencies
- Functions called/Symbols referenced:
  - mmerror (error reporting for unrecognized descriptor types)
  - ECPGdtype enumeration values (ECPGd_count, ECPGd_data, ECPGd_di_code, etc.)
  - PARSE_ERROR, ET_ERROR (error classification constants)
- Called from (representative examples):
  - [output_get_descr](../o/output_get_descr.md) (generates GET DESCRIPTOR statements)
  - [output_set_descr](../o/output_set_descr.md) (generates SET DESCRIPTOR statements)

## Notes and Other Information
- Returns string literals that match the enumeration names exactly (e.g., "ECPGd_count" for ECPGd_count)
- Comprehensive coverage of all PostgreSQL descriptor item types including:
  - Basic data attributes (count, data, type, length)
  - Precision and scale information (precision, scale, di_precision)
  - Indicator and nullable status (indicator, nullable)
  - Return value attributes (ret_length, ret_octet)
  - Metadata (name, key_member, cardinality)
- The function includes unreachable break statements after some return statements (likely for consistency/completeness)
- Returns NULL after error reporting for unrecognized types, though this may not be reached in practice
- Essential for generating proper ECPG runtime calls in the preprocessed C code
- Used specifically in SQL descriptor handling where dynamic attribute access is required