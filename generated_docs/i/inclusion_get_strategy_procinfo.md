# inclusion_get_strategy_procinfo

## Location
[src/backend/access/brin/brin_inclusion.c:608-661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_inclusion.c#L608-L661)

## Overview
A static helper function that caches and returns procedure information for given strategy numbers in BRIN inclusion opclasses, optimizing repeated access to operator procedures through syscache caching.

## Definition


## Detailed Description
This function serves as a procedure lookup and caching mechanism for BRIN inclusion opclasses. It retrieves the procedure corresponding to a given sub-type and strategy number, where the index's data type acts as the left-hand side of the operator and the provided sub-type as the right-hand side. The function implements an intelligent caching strategy to avoid repetitive syscache lookups by storing procedures for the last accessed sub-type in the opaque structure.

The function enforces strict type matching between the opclass data type and column/expression data type, throwing errors when implicit casting would be required. This design choice encourages proper configuration where the storage data type matches the opclass data type.

The caching mechanism invalidates all stored procedures when the sub-type changes, ensuring consistency while maximizing performance for repeated access patterns with the same sub-type.

## Parameters / Member Variables
- : BRIN descriptor containing index metadata and opclass information
- : Attribute number (1-based) identifying the specific column in the index
- : OID of the sub-type to be used as the right-hand side of the operator
- : Strategy number (1 to RTMaxStrategyNumber) identifying the specific operator within the opclass

## Dependencies
- Functions called/Symbols referenced:
  - [BrinDesc](../B/BrinDesc.md)
  - [InclusionOpaque](../I/InclusionOpaque.md)
  - RTMaxStrategyNumber
  - TupleDescAttr
  - [SearchSysCache4](../S/SearchSysCache4.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - Int16GetDatum
  - HeapTupleIsValid
  - [DatumGetObjectId](../D/DatumGetObjectId.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - RegProcedureIsValid
  - [get_opcode](../g/get_opcode.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)

- Called from (representative examples):
  - [brin_inclusion_consistent](../b/brin_inclusion_consistent.md) (multiple calls for different strategy operations)

## Notes and Other Information
- This function mirrors the design of  and changes should be synchronized between both implementations
- The caching strategy is optimized for scenarios where multiple operations use the same sub-type consecutively
- Type casting is intentionally not supported to encourage proper opclass configuration
- The function assumes proper opclass configuration and will throw errors for missing pg_amop entries
- Strategy numbers must be within the valid range (1 to RTMaxStrategyNumber)
- The function operates within the BRIN descriptor's memory context for procedure information storage