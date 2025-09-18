# IsBinaryCoercible

## Location
[src/backend/parser/parse_coerce.c:3032-3046](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L3032-L3046)

## Overview
Checks if a source type can be binary-coercible to a target type without requiring a conversion function call, serving as an implementation shortcut for type conversion.

## Definition


## Detailed Description
IsBinaryCoercible determines whether one PostgreSQL data type can be directly converted to another without invoking a conversion function. This concept allows the system to optimize type conversions by directly exchanging values when it's safe to do so.

The function relies on PostgreSQL's pg_cast system catalog to determine binary coercibility. Two types are considered binary-coercible if there exists an implicitly invokable, no-function-needed pg_cast entry between them. Additionally, domains are always binary-coercible to their base types (but not vice versa), and special handling exists for polymorphic types.

This function replaced the older IsBinaryCompatible() function, which was symmetric. The newer approach respects the asymmetric nature of pg_cast entries, making operand order significant.

## Parameters / Member Variables
- : The OID of the source data type to convert from
- : The OID of the target data type to convert to

## Dependencies
- Functions called/Symbols referenced:
  - [IsBinaryCoercibleWithCast](IsBinaryCoercibleWithCast.md)
- Called from (representative examples):
  - [check_hash_func_signature](../c/check_hash_func_signature.md) (src/backend/access/hash/hashvalidate.c:308)
  - [AggregateCreate](../A/AggregateCreate.md) (src/backend/catalog/pg_aggregate.c:263, 306)
  - [compatible_oper](../c/compatible_oper.md) (src/backend/parser/parse_oper.c:463, 464)
  - [ExecInitAgg](../E/ExecInitAgg.md) (src/backend/executor/nodeAgg.c:3932)
  - [ri_HashCompareOp](../r/ri_HashCompareOp.md) (src/backend/utils/adt/ri_triggers.c:2978)

## Notes and Other Information
- Introduced in PostgreSQL 7.3 to replace the hardwired binary compatibility system
- Before version 7.4, this was used as a heuristic for resolving overloaded functions and operators, which is now considered a bad practice
- The function is essentially a wrapper around IsBinaryCoercibleWithCast that discards the cast OID information
- Domain constraint checks are still required when converting from base type to domain type
- The asymmetric nature means IsBinaryCoercible(A, B) may differ from IsBinaryCoercible(B, A)