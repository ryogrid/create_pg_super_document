# InclusionOpaque

## Location
[src/backend/access/brin/brin_inclusion.c:76-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_inclusion.c#L76-L82)

## Overview
InclusionOpaque is a structure that holds cached operator class information for BRIN inclusion indexes, providing fast access to support procedures and strategy operators.

## Definition

```c
typedef struct InclusionOpaque
{
	FmgrInfo	extra_procinfos[INCLUSION_MAX_PROCNUMS];
	bool		extra_proc_missing[INCLUSION_MAX_PROCNUMS];
	Oid			cached_subtype;
	FmgrInfo	strategy_procinfos[RTMaxStrategyNumber];
} InclusionOpaque;
```
## Detailed Description
The InclusionOpaque structure serves as a cache for operator class information specific to BRIN inclusion indexes. It stores function manager information for both extra support procedures and strategy operators, along with flags indicating missing procedures and the cached subtype OID. This caching mechanism improves performance by avoiding repeated lookups of operator class procedures during index operations.

The structure is part of the BRIN (Block Range Index) inclusion operator class implementation, which supports containment and overlap operations on geometric and other data types that have inclusion relationships.

## Parameters / Member Variables
- : Array of FmgrInfo structures containing cached information for additional support procedures (up to INCLUSION_MAX_PROCNUMS=4)
- : Boolean array indicating which extra support procedures are missing from the operator class
- : OID of the cached subtype for polymorphic operator classes
- : Array of FmgrInfo structures for strategy operators (up to RTMaxStrategyNumber entries)

## Dependencies
- Functions called/Symbols referenced:
  - INCLUSION_MAX_PROCNUMS (constant: 4)
  - RTMaxStrategyNumber (constant for maximum strategy numbers)
  - [FmgrInfo](../F/FmgrInfo.md) (function manager info structure)
  - Oid (object identifier type)

- Called from (representative examples):
  - [brin_inclusion_opcinfo](../b/brin_inclusion_opcinfo.md)
  - [inclusion_get_procinfo](../i/inclusion_get_procinfo.md)
  - [inclusion_get_strategy_procinfo](../i/inclusion_get_strategy_procinfo.md)

## Notes and Other Information
- Located in src/backend/access/brin/brin_inclusion.c:76-82
- This structure is used internally by the BRIN inclusion operator class implementation
- The INCLUSION_MAX_PROCNUMS constant is set to 4, representing the maximum number of additional support procedures needed
- Strategy procedures are indexed by strategy number, with RTMaxStrategyNumber defining the maximum
- The caching mechanism helps avoid repeated catalog lookups during index scan operations