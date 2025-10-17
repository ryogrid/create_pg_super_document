# uuid_abbrev_convert

## Location
[src/backend/utils/adt/uuid.c:358-394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/uuid.c#L358-L394)

## Overview
A conversion function that transforms UUID values into abbreviated keys for optimized sorting by packing the first bytes of UUID data into a Datum for faster comparison.

## Definition
```c
static Datum uuid_abbrev_convert(Datum original, SortSupport ssup)
```

## Detailed Description
This function is the core of PostgreSQL's UUID sorting optimization. It converts full UUID representations into abbreviated keys by extracting the first `sizeof(Datum)` bytes from the UUID data and packing them into a single Datum value. This abbreviated representation allows for much faster sorting comparisons while maintaining correct sort order.

The function employs several key techniques:
1. **Byte packing**: Copies the first 4 or 8 bytes (depending on platform) of UUID data into a Datum
2. **Cardinality tracking**: Updates HyperLogLog estimation during the sampling phase to help determine abbreviation effectiveness
3. **Endian conversion**: Applies byte-swapping on little-endian machines to ensure proper unsigned integer comparison semantics

## Parameters / Member Variables
- `original`: The original UUID value as a Datum
- `ssup`: Sort support structure containing optimization state and callbacks

## Dependencies
- Functions called/Symbols referenced:
  - [SortSupport](../S/SortSupport.md) (structure type)
  - uuid_sortsupport_state (structure type) 
  - [pg_uuid_t](../p/pg_uuid_t.md) (UUID structure type)
  - [DatumGetUUIDP](../D/DatumGetUUIDP.md) (conversion macro)
  - SIZEOF_DATUM (platform-specific size macro)
  - [addHyperLogLog](../a/addHyperLogLog.md) (cardinality estimation function)
  - [hash_uint32](../h/hash_uint32.md) (hash function)
  - [DatumGetUInt32](../D/DatumGetUInt32.md) (conversion macro)
  - DatumBigEndianToNative (endian conversion macro)
- Called from (representative examples):
  - [uuid_sortsupport](uuid_sortsupport.md) (as convert callback assignment)

## Notes and Other Information
- The abbreviated key uses only the first 4/8 bytes of the 16-byte UUID, which is sufficient for most sorting scenarios
- Endian conversion ensures compatibility with `ssup_datum_unsigned_cmp()` on all platforms
- During the estimation phase, contributes hash values to HyperLogLog for cardinality analysis
- The conversion maintains sort order correctness while enabling significantly faster comparisons
- Platform-specific handling ensures optimal performance on both 32-bit and 64-bit systems

## Simplified Source

```c
static Datum uuid_abbrev_convert(Datum original, SortSupport ssup) {
    uuid_sortsupport_state *uss = ssup->ssup_extra;
    pg_uuid_t *uuid = DatumGetUUIDP(original);
    Datum result;

    // Pack first bytes of UUID into Datum for fast comparison
    memcpy(&result, uuid->data, sizeof(Datum));
    uss->input_count += 1;

    // Update cardinality estimation if still sampling
    if (uss->estimating) {
        uint32 hash_input;

        // Extract hash input based on platform word size
        #if SIZEOF_DATUM == 8
            hash_input = (uint32) result ^ (uint32) ((uint64) result >> 32);
        #else
            hash_input = (uint32) result;
        #endif

        addHyperLogLog(&uss->abbr_card, DatumGetUInt32(hash_uint32(hash_input)));
    }

    // Convert to native byte order for proper unsigned comparison
    result = DatumBigEndianToNative(result);

    return result;
}
```