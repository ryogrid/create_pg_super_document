# inv_seek

## Location
[src/backend/storage/large_object/inv_api.c:426-474](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/large_object/inv_api.c#L426-L474)

## Overview
Changes the current read/write position within a large object, similar to the POSIX lseek() function, with support for absolute, relative, and end-relative positioning.

## Definition

```c
int64
inv_seek(LargeObjectDesc *obj_desc, int64 offset, int whence)
```
## Detailed Description
The  function provides positioning control within a large object, allowing applications to move the current offset to any valid location. It supports three positioning modes similar to standard file operations: absolute positioning from the beginning (SEEK_SET), relative positioning from the current location (SEEK_CUR), and positioning relative to the end of the object (SEEK_END).

The function performs bounds checking to ensure the new position is valid, rejecting negative offsets and positions beyond the maximum large object size. For SEEK_END operations, it calls  to determine the current size of the large object. The function allows seeking beyond the current end of the object, which enables sparse large objects with gaps.

## Parameters / Member Variables
- `*obj_desc`: Pointer to the LargeObjectDesc structure for the large object
- `offset`: The offset value, interpretation depends on the whence parameter
- `whence`: Positioning mode (SEEK_SET, SEEK_CUR, or SEEK_END)
## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid (for assertion checking)
  - [inv_getsize](inv_getsize.md) (to get the current size for SEEK_END operations)
  - ereport (for error reporting)
  - [errmsg_internal](../e/errmsg_internal.md) (for internal error messages)
  - MAX_LARGE_OBJECT_SIZE (maximum allowed position)
  - INT64_FORMAT (for formatting 64-bit integers in error messages)
- Called from (representative examples):
  - [be_lo_lseek](../b/be_lo_lseek.md)
  - [be_lo_lseek64](../b/be_lo_lseek64.md)
  - [lo_get_fragment_internal](../l/lo_get_fragment_internal.md)
  - [be_lo_put](../b/be_lo_put.md)

## Notes and Other Information
- No explicit permission check is performed since seek/tell operations are allowed with either read or write permissions
- The function allows seeking beyond the current end of the large object, enabling sparse objects
- Overflow in offset calculations is possible but rejected by the negative result check
- Returns the new absolute offset position after the seek operation
- Uses errmsg_internal for error messages to avoid exposing INT64_FORMAT in translatable strings
- The maximum seek position is limited by MAX_LARGE_OBJECT_SIZE constant
- Position changes are immediately reflected in the obj_desc->offset field

## Simplified Source

```c
int64 inv_seek(LargeObjectDesc *obj_desc, int64 offset, int whence) {
    int64 newoffset;

    Assert(PointerIsValid(obj_desc));

    // Calculate new position based on seek mode
    switch (whence) {
        case SEEK_SET:
            newoffset = offset;  // Absolute position from start
            break;
        case SEEK_CUR:
            newoffset = obj_desc->offset + offset;  // Relative to current position
            break;
        case SEEK_END:
            newoffset = inv_getsize(obj_desc) + offset;  // Relative to end
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("invalid whence setting: %d", whence)));
            newoffset = 0;
            break;
    }

    // Validate the new position is within acceptable bounds
    if (newoffset < 0 || newoffset > MAX_LARGE_OBJECT_SIZE)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg_internal("invalid large object seek target: " INT64_FORMAT,
                                      newoffset)));

    // Update descriptor and return new position
    obj_desc->offset = newoffset;
    return newoffset;
}
```