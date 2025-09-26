# convertJsonbValue

## Location
[src/backend/utils/adt/jsonb_util.c:1596-1620](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1596-L1620)

## Overview
A recursive static function that serves as the main dispatcher for serializing individual JsonbValue structures into binary JSONB format, handling the conversion of scalars, arrays, and objects.

## Definition

```c
struct the header Jentry and store it in the beginning of the
	 * variable-length payload.
	 */
	containerhead = nElems | JB_FARRAY;
```
## Detailed Description
The `convertJsonbValue` function is the central recursive workhorse of JSONB serialization. It acts as a type dispatcher that examines the type of a JsonbValue and delegates to appropriate specialized conversion functions (`convertJsonbScalar`, `convertJsonbArray`, or `convertJsonbObject`). The function includes important safety checks such as stack depth monitoring to prevent stack overflow during deep recursion, and validation that input values don't already have binary representation. It fills in the JEntry header with length and type information for the serialized value, making it a critical component in building the JSONB binary format's header structure.

## Parameters / Member Variables
- `buffer`: A StringInfo structure where the serialized JSONB data will be written
- `header`: Pointer to a JEntry structure that will be filled with metadata (length, type bits) for this value
- `val`: Pointer to the JsonbValue structure to be converted
- `level`: Integer representing the current recursion depth, used primarily for debugging purposes

## Dependencies
- Functions called/Symbols referenced:
  - JEntry (structure type)
  - [check_stack_depth](check_stack_depth.md)
  - IsAJsonbScalar
  - [convertJsonbScalar](convertJsonbScalar.md)
  - jbvArray (enum value)
  - [convertJsonbArray](convertJsonbArray.md)
  - jbvObject (enum value)  
  - [convertJsonbObject](convertJsonbObject.md)
  - elog (for error reporting)
- Called from (representative examples):
  - [convertToJsonb](convertToJsonb.md)
  - [convertJsonbArray](convertJsonbArray.md) (recursive)
  - [convertJsonbObject](convertJsonbObject.md) (recursive)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the jsonb_util.c compilation unit
- Includes stack depth checking to prevent stack overflow during deep recursive conversions
- The function validates that JsonbValue inputs never have jbvBinary type, as binary representations are produced by the conversion functions themselves
- The JEntry header is populated with value length and type information by the delegated conversion functions
- Acts as the central dispatch point for the recursive JSONB serialization process
- The level parameter is primarily used for debugging and can help track recursion depth
- Throws an ERROR if an unknown JsonbValue type is encountered
- Critical for maintaining the integrity of JSONB's binary format through proper header management
- The function handles the recursive nature of JSONB structures (arrays containing arrays/objects, objects containing arrays/objects, etc.)