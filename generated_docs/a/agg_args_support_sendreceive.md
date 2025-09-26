# agg_args_support_sendreceive

## Location
[src/backend/parser/parse_agg.c:1968-2034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L1968-L2034)

## Overview
Determines whether all non-byval (pass-by-reference) argument types of an aggregate function have the required send and receive functions for serialization.

## Definition
```c
bool agg_args_support_sendreceive(Aggref *aggref)
```

## Detailed Description
This function examines all arguments of an aggregate function to determine if they support serialization and deserialization through send and receive functions. This capability is crucial for operations like parallel aggregation where intermediate results need to be transferred between processes.

The function iterates through each argument of the aggregate and performs the following checks:
1. Extracts the data type of each argument expression
2. Handles the special case of RECORD types, which are explicitly rejected due to limitations in the deserialization process (array_agg_deserialize cannot handle typmod information properly)
3. For non-byval types, verifies that both typsend and typreceive function OIDs are valid in the pg_type catalog

By-value types are automatically considered supported since they don't require special serialization functions.

## Parameters / Member Variables
- `aggref`: Pointer to an Aggref node containing the aggregate function call information with its arguments

## Dependencies
- Functions called/Symbols referenced:
  - `[Aggref](../A/Aggref.md)` (struct type)
  - `Form_pg_type` (struct type for pg_type catalog entries)
  - `[exprType](../e/exprType.md)()` (extracts type from expression nodes)
  - [SearchSysCache1](../S/SearchSysCache1.md)() (catalog cache lookup)
  - `HeapTupleIsValid()` (validates tuple)
  - `GETSTRUCT()` (extracts struct from heap tuple)
  - `OidIsValid()` (validates OID)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)() (cache cleanup)
  - `RECORDOID` (constant for RECORD type)
- Called from (representative examples):
  - [preprocess_aggref](../p/preprocess_aggref.md) (in prepagg.c)

## Notes and Other Information
- RECORD types are specifically excluded because record_recv requires correct typmod information to identify anonymous record types, which array_agg_deserialize cannot provide
- The function is primarily used in the context of parallel aggregation planning to determine if an aggregate can be safely parallelized
- Proper memory management through SearchSysCache1/ReleaseSysCache pairs
- Returns false immediately upon finding any unsupported type, implementing short-circuit evaluation for efficiency