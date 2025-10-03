# _hash_convert_tuple

## Location
[src/backend/access/hash/hashutil.c:318-349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashutil.c#L318-L349)

## Overview
Converts raw user data values into hash index tuple format by computing hash keys and preparing data for index storage.

## Definition
```c
bool _hash_convert_tuple(Relation index,
                        Datum *user_values, bool *user_isnull,
                        Datum *index_values, bool *index_isnull)
```

## Detailed Description
This function serves as the bridge between user data and hash index storage format. It transforms the original user data into the format required for hash index tuples, where the actual data values are replaced with their computed hash keys.

The conversion process involves:
1. **Null Value Handling**: Rejects null values since hash indexes don't support null indexing (consistent with the '=' operator being strict)
2. **Hash Key Computation**: Uses  to compute the hash value from the user data
3. **Format Conversion**: Converts the hash key to a Datum and marks it as non-null

The function is designed to handle the standard case of single-column hash indexes, though it's structured to potentially support multiple input columns in the future.

## Parameters
- `index`: The hash index relation for which conversion is being performed
- `user_values`: Array of input data values from the user
- `user_isnull`: Array of null flags for the input values  
- `index_values`: Output array for converted index tuple values
- `index_isnull`: Output array for null flags of converted values

## Dependencies
- Functions called/Symbols referenced:
  - [_hash_datum2hashkey](_hash_datum2hashkey.md)
  - [UInt32GetDatum](../U/UInt32GetDatum.md)
- Called from (representative examples):
  - [hashbuildCallback](hashbuildCallback.md)
  - [hashinsert](hashinsert.md)

## Simplified Source
```c
bool _hash_convert_tuple(Relation index,
                        Datum *user_values, bool *user_isnull,
                        Datum *index_values, bool *index_isnull) {
    // Hash indexes don't support null values
    if (user_isnull[0])
        return false;

    // Compute hash key from user data
    uint32 hashkey = _hash_datum2hashkey(index, user_values[0]);

    // Store hash key as index value
    index_values[0] = UInt32GetDatum(hashkey);
    index_isnull[0] = false;

    return true;
}
```

## Notes and Other Information
This function is essential for hash index insertion operations and bulk index building. The return value indicates whether the conversion was successful - a false return means the data contains null values and should not be indexed. The function's design reflects hash indexes' fundamental limitation of not supporting null values, which is acceptable given that hash indexes only support equality searches with strict operators.