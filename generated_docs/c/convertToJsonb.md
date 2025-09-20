# convertToJsonb

## Location
[src/backend/utils/adt/jsonb_util.c:1554-1595](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1554-L1595)

## Overview
A static function that converts a JsonbValue structure into a complete Jsonb binary representation, creating a properly formatted and varlena-header-compliant JSONB data structure.

## Definition

```c
struct must contain enough information to tell what kind
	 * of value it is.
	 */

	res = (Jsonb *) buffer.data;
```
## Detailed Description
The `convertToJsonb` function is the primary converter that transforms in-memory JsonbValue representations into the final binary JSONB format used by PostgreSQL. It initializes a StringInfo buffer, reserves space for the varlena header (required for variable-length PostgreSQL data types), and then calls `convertJsonbValue` to recursively serialize the entire value tree. The function handles the critical task of setting up the proper JSONB structure, including discarding the root JEntry (since root containers must be self-describing) and setting the correct varlena size. The result is a palloc'd Jsonb structure ready for storage or transmission.

## Parameters / Member Variables
- `val`: Pointer to a JsonbValue structure that needs to be converted to binary JSONB format

## Dependencies
- Functions called/Symbols referenced:
  - JEntry (structure type)
  - Jsonb (structure type)  
  - jbvBinary (enum value)
  - initStringInfo
  - [reserveFromBuffer](../r/reserveFromBuffer.md)
  - [convertJsonbValue](convertJsonbValue.md)
  - SET_VARSIZE
- Called from (representative examples):
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the jsonb_util.c compilation unit
- Returns a palloc'd Jsonb structure that must be managed by PostgreSQL's memory management system
- Includes an assertion to ensure the input JsonbValue doesn't already have binary representation
- The root JEntry is intentionally discarded because root JsonbContainer structures must be self-describing
- Reserves space for VARHDRSZ (varlena header size) to comply with PostgreSQL's variable-length data type format
- Sets the proper varlena size using SET_VARSIZE macro before returning
- The function is the main entry point for converting from the internal JsonbValue representation to the on-disk/network JSONB format
- Used primarily by JsonbValueToJsonb, which is the public interface for this conversion