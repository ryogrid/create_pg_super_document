# JsonbExtractScalar

## Location
[src/backend/utils/adt/jsonb.c:1968-2007](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1968-L2007)

## Overview
Extracts a scalar value from a raw-scalar pseudo-array JSONB container, handling the special case where root scalars are stored as single-element arrays.

## Definition
```c
bool JsonbExtractScalar(JsonbContainer *jbc, JsonbValue *res)
```

## Detailed Description
This function extracts scalar values from JSONB containers that represent root scalars. In PostgreSQL's JSONB implementation, root scalar values are stored internally as arrays containing a single element with a special rawScalar flag. This function unwraps such pseudo-arrays to extract the actual scalar value.

The function first validates that the container is both an array and marked as scalar. If these conditions aren't met, it sets the result type to indicate the actual container type (array or object) and returns false. If the validation passes, it uses a JsonbIterator to walk through the pseudo-array structure, extracting the single scalar element and performing comprehensive assertions to ensure the container structure matches expectations.

## Parameters / Member Variables
- `jbc`: JsonbContainer pointer to the container that may hold a raw scalar
- `res`: JsonbValue pointer to store the extracted scalar value or type information

## Dependencies
- Functions called/Symbols referenced:
  - JsonContainerIsArray
  - JsonContainerIsScalar
  - [JsonbIteratorInit](JsonbIteratorInit.md)
  - [JsonbIteratorNext](JsonbIteratorNext.md)
  - IsAJsonbScalar
- Types/Constants referenced:
  - [JsonbContainer](JsonbContainer.md)
  - [JsonbValue](JsonbValue.md)
  - JsonbIterator
  - JsonbIteratorToken
  - jbvArray, jbvObject
  - WJB_BEGIN_ARRAY, WJB_ELEM, WJB_END_ARRAY, WJB_DONE
- Called from (representative examples):
  - [jsonb_bool](../j/jsonb_bool.md)
  - [jsonb_numeric](../j/jsonb_numeric.md)
  - [jsonb_int2](../j/jsonb_int2.md), jsonb_int4, jsonb_int8
  - [jsonb_float4](../j/jsonb_float4.md), jsonb_float8
  - [JsonbUnquote](JsonbUnquote.md)
  - [executeJsonPath](../e/executeJsonPath.md)

## Notes and Other Information
- Returns true if extraction succeeds, false if the container doesn't contain a raw scalar
- Root scalars are stored as single-element arrays with rawScalar flag set to true
- Contains extensive assertions to validate the expected pseudo-array structure
- Used primarily for type conversion functions that extract scalar values from JSONB
- Located in src/backend/utils/adt/jsonb.c:1968-2007
- Critical for proper handling of JSONB scalar values in PostgreSQL's type system

## Simplified Source

```c
bool
JsonbExtractScalar(JsonbContainer *jbc, JsonbValue *res)
{
    JsonbIterator *it;
    JsonbValue tmp;

    // Validate container is both array and scalar
    if (!JsonContainerIsArray(jbc) || !JsonContainerIsScalar(jbc))
    {
        // Set result type and return false for non-scalar containers
        res->type = JsonContainerIsArray(jbc) ? jbvArray : jbvObject;
        return false;
    }

    // Initialize iterator for pseudo-array traversal
    it = JsonbIteratorInit(jbc);

    // Navigate through the single-element array structure
    JsonbIteratorNext(&it, &tmp, true);     // WJB_BEGIN_ARRAY
    JsonbIteratorNext(&it, res, true);      // WJB_ELEM (the scalar value)
    JsonbIteratorNext(&it, &tmp, true);     // WJB_END_ARRAY
    JsonbIteratorNext(&it, &tmp, true);     // WJB_DONE

    return true;
}
```