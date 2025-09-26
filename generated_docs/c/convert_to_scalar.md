# convert_to_scalar

## Location
[src/backend/utils/adt/selfuncs.c:4318-4464](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L4318-L4464)

## Overview
Converts non-NULL values of various PostgreSQL data types to a scalar comparison scale needed by selectivity estimation functions, handling numeric, string, bytea, time, and network data types.

## Definition

```c
static bool
convert_to_scalar(Datum value, Oid valuetypid, Oid collid, double *scaledvalue,
				  Datum lobound, Datum hibound, Oid boundstypid,
				  double *scaledlobound, double *scaledhibound)
```
## Detailed Description
This function serves as a central dispatcher for converting PostgreSQL data values to a common scalar representation used in selectivity estimation. It handles multiple data type categories:

1. **Numeric Types**: All numeric data types (bool, int2, int4, int8, float4, float8, numeric, and various OID types) are converted to double values using . NUMERIC values outside double range are clamped to ±HUGE_VAL.

2. **String Types**: String types (char, bpchar, varchar, text, name) are handled through string-to-scalar conversion that considers collation settings.

3. **Bytea Type**: Binary data is processed separately with its own conversion logic.

4. **Time Types**: Temporal data types (timestamp, timestamptz, date, interval, time, timetz) are converted via , typically normalizing to seconds or converting to int64 timestamps.

5. **Network Types**: Network address types (inet, cidr, macaddr, macaddr8) are converted through .

The function processes three values simultaneously (value, lobound, hibound) because string conversions require knowledge of the range boundaries for proper scaling.

## Parameters
- : The primary data value to convert
- : OID of the value's data type
- : Collation ID for string types
- : Output pointer for the converted primary value
- : Lower boundary value for range context
- : Upper boundary value for range context  
- : OID of the boundary values' data type
- : Output pointer for the converted lower bound
- : Output pointer for the converted upper bound

## Dependencies
- Functions called:
  - [convert_numeric_to_scalar](convert_numeric_to_scalar.md)
  - [convert_string_datum](convert_string_datum.md)
  - [convert_string_to_scalar](convert_string_to_scalar.md)
  - [convert_bytea_to_scalar](convert_bytea_to_scalar.md)
  - [convert_timevalue_to_scalar](convert_timevalue_to_scalar.md)
  - [convert_network_to_scalar](convert_network_to_scalar.md)
- Called from:
  - [ineq_histogram_selectivity](../i/ineq_histogram_selectivity.md) (in selfuncs.c:1223)

## Notes and Other Information
- Returns true if conversion is successful, false if the data type is not supported
- The function is acknowledged as a "hack" - ideally conversions would be looked up in pg_type
- Handles binary-compatible types by assuming similar semantics, which may be incorrect for signed vs unsigned interpretations
- When conversion fails or type is unsupported, sets all output values to 0 and returns false
- The value and boundary types don't need to be identical, allowing for binary-compatible type comparisons
- Extensions using scalarineqsel as an estimator for unsupported types will get a false return rather than an error