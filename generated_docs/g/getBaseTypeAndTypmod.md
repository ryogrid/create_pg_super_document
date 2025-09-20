# getBaseTypeAndTypmod

## Location
[src/backend/utils/cache/lsyscache.c:2538-2577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2538-L2577)

## Overview
Resolves the base type of a PostgreSQL type by unwinding domain type hierarchies and returns the base type OID along with its type modifier.

## Definition

```c
Oid
getBaseTypeAndTypmod(Oid typid, int32 *typmod)
```
## Detailed Description
This function traverses the PostgreSQL domain type hierarchy to find the underlying base type. In PostgreSQL, domains are user-defined data types that are based on another data type (which can be another domain, creating a stack). The function iteratively follows the domain chain until it reaches a non-domain type, which is considered the base type.

The function modifies the typmod parameter in-place as it traverses the domain stack. For domain types, the applied typmod should be -1 at every level above the bottommost base type, and the actual typmod is stored with the base type.

The implementation uses a loop to handle nested domains efficiently, looking up each type in the system cache (pg_type) and checking if it's a domain type. If it is, the function follows the typbasetype reference and updates the typmod accordingly.

## Parameters / Member Variables
- : Input type OID that may be a domain type
- : Pointer to type modifier; updated in-place to reflect the base type's typmod

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract tuple structure)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_type (pg_type tuple structure)
  - TYPTYPE_DOMAIN (domain type constant)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID conversion macro)

- Called from (representative examples):
  - [SendRowDescriptionMessage](../S/SendRowDescriptionMessage.md) (src/backend/access/common/printtup.c:208)
  - [ATExecAddColumn](../A/ATExecAddColumn.md) (src/backend/commands/tablecmds.c:7289)
  - [coerce_type](../c/coerce_type.md) (src/backend/parser/parse_coerce.c:268, 430)
  - [transformTypeCast](../t/transformTypeCast.md) (src/backend/parser/parse_expr.c:2725)
  - [lookup_type_cache](../l/lookup_type_cache.md) (src/backend/utils/cache/typcache.c:864)

## Notes and Other Information
- The function assumes that typmod is -1 when called with a domain type, as enforced by an Assert
- Used extensively in type coercion and casting operations throughout the PostgreSQL parser and planner
- Essential for maintaining type safety when working with user-defined domain types
- The function is located in lsyscache.c, which provides cached access to system catalog information