# convert_network_to_scalar

## Location
src/backend/utils/adt/network.c: 1502 - 1568

## Overview
Converts network datatype values (inet, cidr, macaddr, macaddr8) to approximate scalar values for selectivity estimation in query planning.

## Definition
```c
double convert_network_to_scalar(Datum value, Oid typid, bool *failure)
```

## Detailed Description
This function converts various network-related data types to approximate scalar (double) values that can be used by PostgreSQL's query planner for estimating selectivity of inequality operators. The conversion creates a numeric representation that preserves ordering relationships, enabling the planner to make informed decisions about query optimization. For inet/cidr types, it uses the address family and initial bytes of the address. For MAC addresses, it converts the entire address to a scalar value.

## Parameters / Member Variables
- `value`: The Datum containing the network value to convert
- `typid`: The OID of the data type (INETOID, CIDROID, MACADDROID, or MACADDR8OID)
- `failure`: Pointer to boolean flag set to true if conversion fails for unsupported type

## Dependencies
- Functions called/Symbols referenced:
  - `DatumGetInetPP` - extracts inet value from Datum
  - `DatumGetMacaddrP` - extracts macaddr value from Datum
  - `DatumGetMacaddr8P` - extracts macaddr8 value from Datum
  - `ip_family` - gets address family from inet structure
  - `ip_addr` - gets address bytes from inet structure
  - `PGSQL_AF_INET` - constant for IPv4 address family
- Called from (representative examples):
  - `convert_to_scalar` in src/backend/utils/adt/selfuncs.c

## Notes and Other Information
- Used specifically for query planner selectivity estimation, not for general conversion
- For IPv4 addresses: uses family + first 4 address bytes
- For IPv6 addresses: uses family + first 5 address bytes (note: doesn't use full 16-byte address for performance)
- For MAC addresses: converts entire 6-byte or 8-byte address to scalar
- Returns 0 and sets failure flag for unsupported data types
- The scalar values maintain ordering relationships necessary for inequality comparisons
- Located in src/backend/utils/adt/network.c:1502-1568