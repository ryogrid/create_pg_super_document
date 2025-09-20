# getFormattedOperatorName

## Location
[src/bin/pg_dump/pg_dump.c:13222-13251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L13222-L13251)

## Overview
Retrieves and formats an operator name for the given operator OID, producing a fully qualified operator reference suitable for SQL commands.

## Definition

```c
static char *
getFormattedOperatorName(const char *oproid)
```
## Detailed Description
This function takes an operator OID in string form and returns a formatted operator name with the pattern "OPERATOR(schema.oprname)". The function always schema-qualifies the operator name to avoid ambiguity issues that could arise in corner cases, such as when an operator and its negator exist in different schemas.

The function performs OID validation and operator lookup, returning NULL for invalid references or if the operator cannot be found. It uses the internal operator information structures to construct the properly formatted name.

## Parameters / Member Variables
- : String representation of the operator OID to look up and format

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (for string comparison)
  - [findOprByOid](../f/findOprByOid.md) (to locate operator information by OID)
  - atooid (to convert string OID to Oid type)  
  - pg_log_warning (for error logging)
  - [psprintf](../p/psprintf.md) (for formatted string creation)
  - [fmtId](../f/fmtId.md) (for proper identifier formatting)
- Called from (representative examples):
  - [dumpOpr](../d/dumpOpr.md) (multiple calls for operator references)
  - [dumpAgg](../d/dumpAgg.md) (for aggregate operator references)
  - fmtQualifiedDumpable

## Notes and Other Information
- Returns NULL for invalid OID references (represented as "0")
- Always includes schema qualification to avoid ambiguity
- The returned string must be freed by the caller
- Produces format suitable for SQL commands where operator argument types can be inferred from context
- Part of PostgreSQL's pg_dump utility for generating proper operator references in dump output
- Handles error cases by logging warnings and returning NULL