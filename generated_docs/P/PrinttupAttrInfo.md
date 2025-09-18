# PrinttupAttrInfo

## Location
src/backend/access/common/printtup.c: 52 - 63

## Overview
PrinttupAttrInfo is a structure that holds per-attribute information for PostgreSQL's tuple output formatting, storing metadata needed to convert internal data types to their text or binary output representations.

## Definition


## Detailed Description
PrinttupAttrInfo is a fundamental data structure used in PostgreSQL's tuple printing subsystem to cache attribute-specific information required for efficient data formatting. This structure is part of the printtup destination receiver mechanism, which handles the conversion and output of query results in both text and binary formats.

The structure stores precomputed metadata for each column in a result set, including function OIDs for type conversion, format specifications, and cached function call information. This design enables efficient repeated formatting operations by avoiding redundant type system lookups during tuple output processing.

## Parameters / Member Variables
- : OID of the type's text output function, used when formatting data as text
- : OID of the type's binary output function, used when formatting data in binary protocol
- : Boolean flag indicating whether the type is variable-length and potentially toastable
- : Format code specifying how this column should be formatted (text vs binary)
- : Precomputed function manager information for the selected output function, optimizing repeated calls

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - [FmgrInfo](../F/FmgrInfo.md) (function manager info structure)
  - int16 (16-bit integer type)
  - [bool](../b/bool.md) (boolean type)
- Called from (representative examples):
  - [printtup_prepare_info](../p/printtup_prepare_info.md) (initializes PrinttupAttrInfo arrays)
  - [printtup](../p/printtup.md) (uses PrinttupAttrInfo for tuple formatting)

## Notes and Other Information
- This structure is designed for performance optimization in query result formatting by caching frequently-used type information
- The finfo member contains precomputed function call information, eliminating the need for repeated function lookups during tuple processing
- The structure supports both text and binary output formats through the typoutput/typsend OID fields
- Part of the larger printtup subsystem that handles PostgreSQL's client-server result transmission protocol
- Located in src/backend/access/common/printtup.c:52-63