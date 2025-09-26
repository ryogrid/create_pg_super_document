# reserveFromBuffer

## Location
[src/backend/utils/adt/jsonb_util.c:1484-1509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1484-L1509)

## Overview
reserveFromBuffer reserves a specified number of bytes at the end of a StringInfo buffer, enlarging it if necessary, and returns the offset to the reserved area.

## Definition

```c
static int
reserveFromBuffer(StringInfo buffer, int len)
```
## Detailed Description
This static function is part of the buffer manipulation utilities used by convertJsonb and related functions for JSONB serialization. It reserves space at the end of a StringInfo buffer by first ensuring sufficient capacity through enlargeStringInfo, then advancing the buffer length by the requested amount. The function maintains StringInfo invariants by preserving a trailing null terminator after the reserved space. This design allows callers to efficiently allocate space and fill it later using copyToBuffer(), supporting incremental construction of JSONB binary representations.

## Parameters / Member Variables
- : StringInfo buffer to reserve space from
- : Number of bytes to reserve at the end of the buffer

## Dependencies
- Functions called/Symbols referenced:
  - [enlargeStringInfo](../e/enlargeStringInfo.md) (to ensure buffer capacity)
- Called from (representative examples):
  - [appendToBuffer](../a/appendToBuffer.md)
  - [padBufferToInt](../p/padBufferToInt.md)
  - [convertToJsonb](../c/convertToJsonb.md)
  - [convertJsonbArray](../c/convertJsonbArray.md)
  - [convertJsonbObject](../c/convertJsonbObject.md)

## Notes and Other Information
The function is declared static and scoped to jsonb_util.c. It follows a reserve-then-fill pattern common in buffer management, where space allocation is separated from data copying. The preservation of StringInfo invariants (trailing null terminator) ensures compatibility with standard PostgreSQL string handling utilities. The returned offset can be used by subsequent calls to copyToBuffer() to fill the reserved space with actual data. This approach optimizes memory allocation by reducing the number of buffer resize operations during JSONB construction.