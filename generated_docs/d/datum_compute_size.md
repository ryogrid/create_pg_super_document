# datum_compute_size

## Location
[src/backend/utils/adt/rangetypes.c:2683-2708](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L2683-L2708)

## Overview
A static utility function that calculates the space needed by a datum in serialized form, including any preceding alignment padding.

## Definition


## Detailed Description
This function is part of PostgreSQL's range type serialization infrastructure. It computes the total size required to store a datum in a serialized format by considering various type characteristics such as alignment requirements and storage strategies. The function handles two main cases: packable types that can be converted to short varlena headers for space optimization, and regular types that require standard alignment and length calculations.

For packable types with short varlena capability, it calculates the converted short size without additional alignment padding. For other types, it applies proper alignment using  and adds the appropriate length using .

## Parameters / Member Variables
- : Current accumulated data length before adding this datum
- : The datum value to be sized
- : Whether the type is passed by value
- : Type alignment requirement ('c', 's', 'i', 'd')
- : Type length (-1 for variable length, -2 for cstring, positive for fixed length)
- : Type storage strategy ('p', 'e', 'm', 'x')

## Dependencies
- Functions called/Symbols referenced:
  - TYPE_IS_PACKABLE
  - VARATT_CAN_MAKE_SHORT
  - VARATT_CONVERTED_SHORT_SIZE
  - att_align_datum
  - att_addlength_datum
  - [DatumGetPointer](../D/DatumGetPointer.md)
- Called from (representative examples):
  - [range_serialize](../r/range_serialize.md)

## Notes and Other Information
This is a static function specifically designed for range type serialization. It optimizes storage by checking if variable-length types can be stored in short varlena format, which saves space by avoiding alignment padding. The function is critical for efficient range type storage and is called twice in range_serialize for both lower and upper bound values.