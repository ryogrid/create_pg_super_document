# JsonbValue

## Location
[src/include/utils/jsonb.h:253-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/jsonb.h#L253-L296)

## Overview
JsonbValue is the in-memory representation of PostgreSQL's JSONB data type, providing a convenient deserialized structure for manipulating JSON data in memory.

## Definition

```c
struct JsonbValue
{
	enum jbvType type;			/* Influences sort order */

	union
	{
		Numeric numeric;
		bool		boolean;
		struct
		{
			int			len;
			char	   *val;	/* Not necessarily null-terminated */
		}			string;		/* String primitive type */

		struct
		{
			int			nElems;
			JsonbValue *elems;
			bool		rawScalar;	/* Top-level "raw scalar" array? */
		}			array;		/* Array container type */

		struct
		{
			int			nPairs; /* 1 pair, 2 elements */
			JsonbPair  *pairs;
		}			object;		/* Associative container type */

		struct
		{
			int			len;
			JsonbContainer *data;
		}			binary;		/* Array or object, in on-disk format */

		struct
		{
			Datum		value;
			Oid			typid;
			int32		typmod;
			int			tz;		/* Numeric time zone, in seconds, for
								 * TimestampTz data type */
		}			datetime;
	}			val;
};
```
## Detailed Description
JsonbValue serves as PostgreSQL's in-memory representation for JSONB data, contrasting with the on-disk Jsonb format which has various alignment considerations. This structure provides a convenient deserialized representation that supports using the "val" union across different underlying types during manipulation. JsonbValues can either be shims through which a Jsonb buffer is accessed, or they can be deep copied and passed around independently.

The structure uses a discriminated union approach where the  field determines which member of the  union is active. This design enables efficient type-specific operations while maintaining a unified interface for all JSON value types.

## Parameters / Member Variables
- `type`: Enum jbvType value that determines which union member is active and influences sort order when comparing JSON values
- `val`: Union containing type-specific data, accessed based on the type field
- `val.numeric`: Numeric value (PostgreSQL Numeric type) used when type is jbvNumeric
- `val.boolean`: Boolean value used when type is jbvBool
- `val.string`: String primitive structure used when type is jbvString
  - `val.string.len`: Length of the string in bytes
  - `val.string.val`: Character pointer to string data (not necessarily null-terminated)
- `val.array`: Array container structure used when type is jbvArray
  - `val.array.nElems`: Number of elements in the array
  - `val.array.elems`: Pointer to array of JsonbValue elements
  - `val.array.rawScalar`: Boolean flag indicating if this is a top-level "raw scalar" array
- `val.object`: Associative container structure used when type is jbvObject
  - `val.object.nPairs`: Number of key-value pairs (note: 1 pair equals 2 elements)
  - `val.object.pairs`: Pointer to array of JsonbPair structures
- `val.binary`: Binary format structure used when type is jbvBinary
  - `val.binary.len`: Length of the binary data in bytes
  - `val.binary.data`: Pointer to JsonbContainer in on-disk format
- `val.datetime`: Date/time structure used when type is jbvDatetime
  - `val.datetime.value`: Datum containing the actual date/time value
  - `val.datetime.typid`: Type OID identifying the specific date/time type
  - `val.datetime.typmod`: Type modifier for the date/time type
  - `val.datetime.tz`: Numeric time zone offset in seconds (used for TimestampTz data type)

## Dependencies
- Types referenced:
  - jbvType (enum defining JSON value types)
  - Numeric (PostgreSQL numeric type)
  - [JsonbPair](JsonbPair.md) (key-value pair structure)
  - [JsonbContainer](JsonbContainer.md) (on-disk container format)
  - Datum (PostgreSQL datum type)
  - Oid (PostgreSQL object identifier type)
- Related structures:
  - Jsonb (on-disk representation)
  - [JsonbPair](JsonbPair.md) (object key-value pairs)
  - [JsonbContainer](JsonbContainer.md) (binary container format)

## Notes and Other Information
- The structure is designed for efficient in-memory JSON manipulation while the on-disk Jsonb format prioritizes storage efficiency and alignment
- The  field not only determines union member access but also influences sort order for JSON values
- String values in the structure are not necessarily null-terminated, requiring explicit length tracking
- The  flag in arrays indicates special handling for top-level scalar values stored as single-element arrays
- Binary format support allows direct access to on-disk Jsonb data without full deserialization
- Date/time support includes timezone information for TimestampTz types
- The structure supports PostgreSQL's full range of JSON types including nulls, booleans, numbers, strings, arrays, and objects