# dsa_on_dsm_detach_release_in_place

## Location
[src/backend/utils/mmgr/dsa.c:576-589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L576-L589)

## Overview
A callback function that automatically releases in-place DSA areas when their containing DSM segment is detached, providing automatic cleanup for DSA areas embedded within DSM segments.

## Definition
```c
void dsa_on_dsm_detach_release_in_place(dsm_segment *segment, Datum place)
```

## Detailed Description
This function serves as a DSM detach callback that automatically releases DSA areas created with dsa_create_in_place or dsa_attach_in_place when the containing DSM segment is detached. The function is designed to be compatible with the on_dsm_detach callback interface, accepting a DSM segment parameter (which is ignored) and a Datum containing the memory address of the DSA area.

The function simply extracts the memory address from the Datum parameter and calls dsa_release_in_place to perform the actual cleanup. This provides a convenient way for users to create DSA areas inside existing DSM segments with automatic cleanup when the container segment is destroyed.

## Parameters / Member Variables
- `segment`: The DSM segment being detached (parameter ignored but required for callback interface compatibility)
- `place`: Datum containing the memory address where the DSA area was created

## Dependencies
- Functions called/Symbols referenced:
  - [dsa_release_in_place](dsa_release_in_place.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
- Called from (representative examples):
  - [dsa_create_ext](dsa_create_ext.md) (src/backend/utils/mmgr/dsa.c:448)
  - [dsa_create_in_place_ext](dsa_create_in_place_ext.md) (src/backend/utils/mmgr/dsa.c:486)
  - [dsa_attach](dsa_attach.md) (src/backend/utils/mmgr/dsa.c:528)
  - [dsa_attach_in_place](dsa_attach_in_place.md) (src/backend/utils/mmgr/dsa.c:556)

## Notes and Other Information
- This callback is automatically registered when a DSM segment is provided to dsa_create_in_place or dsa_attach_in_place
- Also registered for all areas created with dsa_create for consistent cleanup behavior
- The segment parameter is ignored but maintained for interface compatibility with on_dsm_detach
- The place parameter must contain the exact memory address where the DSA area was originally created
- Provides automatic resource management for DSA areas embedded within DSM segments
- Ensures proper cleanup even if the application doesn't explicitly release the DSA area

## Simplified Source

```c
void dsa_on_dsm_detach_release_in_place(dsm_segment *segment, Datum place)
{
    dsa_release_in_place(DatumGetPointer(place));
}
```

This is a simple callback function that releases in-place DSA areas when their containing DSM segment is detached. It extracts the memory address from the Datum parameter and delegates to dsa_release_in_place for cleanup.