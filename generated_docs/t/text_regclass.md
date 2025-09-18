# text_regclass

## Location
[src/backend/utils/adt/regproc.c:1774-1796](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1774-L1796)

## Overview
Converts a text representation of a relation name to the regclass PostgreSQL data type, supporting implicit casting for legacy functions like nextval().

## Definition
```c
Datum text_regclass(PG_FUNCTION_ARGS)
```

## Detailed Description
The `text_regclass` function converts a text string containing a relation name (table, view, sequence, etc.) into PostgreSQL's regclass data type, which represents relation object identifiers. This function is designed to support implicit casting from text to regclass, which is particularly important for legacy forms of functions like `nextval()` and related sequence functions.

The function parses the input text as a qualified name (potentially schema.relation), creates a RangeVar structure, and resolves it to the actual relation OID. Unlike normal relation access, this function does not acquire locks on the relation since it might not have permissions and is primarily used for name resolution.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro which provides:
  - `relname` (text): The input text containing the relation name to be converted

## Dependencies
- Functions called/Symbols referenced:
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md)
  - textToQualifiedNameList  
  - RangeVarGetRelid
  - PG_RETURN_OID
- Types used:
  - [RangeVar](../R/RangeVar.md)
- Called from (representative examples):
  - Used internally by PostgreSQL's type system for text-to-regclass conversions
  - Not directly referenced by other user-visible functions

## Notes and Other Information
- Designed to replace CoerceViaIO for text-to-regclass conversions while maintaining implicit cast behavior
- Uses NoLock when resolving the relation to avoid permission issues and unnecessary locking
- Critical for supporting legacy function calls that pass text arguments where regclass is expected
- Part of the broader regproc family handling various registry data types
- Located in src/backend/utils/adt/regproc.c with related registry type functions