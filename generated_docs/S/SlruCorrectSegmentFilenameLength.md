# SlruCorrectSegmentFilenameLength

## Location
[src/backend/access/transam/slru.c:1755-1787](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1755-L1787)

## Overview
A static inline function that validates whether a filename length is appropriate for an SLRU segment file based on the SLRU configuration.

## Definition
```c
static inline bool SlruCorrectSegmentFilenameLength(SlruCtl ctl, size_t len)
```

## Detailed Description
SlruCorrectSegmentFilenameLength is an internal validation function used by SlruScanDirectory to determine if a file in an SLRU directory has a filename length that could correspond to a valid SLRU segment file. The function handles two different naming schemes: modern long segment names (15 characters) and legacy short segment names (4, 5, or 6 characters). The long segment names are part of an ongoing migration to support 64-bit page numbers, while the shorter lengths are maintained for backward compatibility. The function checks the ctl->long_segment_names flag to determine which validation rules to apply.

## Parameters / Member Variables
- `ctl`: SlruCtl structure containing SLRU control information, specifically the long_segment_names flag
- `len`: Size of the filename being validated

## Dependencies  
- Functions called/Symbols referenced:
  - (none - uses only standard length comparisons)
- Called from (representative examples):
  - [SlruScanDirectory](SlruScanDirectory.md)

## Notes and Other Information
- Supports both legacy (4,5,6 character) and modern (15 character) SLRU filename lengths
- The 15-character length corresponds to the SlruFileName() function format for 64-bit page numbers
- Legacy support includes 5-character names added by commit 638cf09e76d and 6-character names added by commit 73c986adde5
- There is an ongoing plan to migrate all SLRUs to 64-bit page numbers, which may eventually deprecate support for shorter names
- Static inline function for performance optimization during directory scanning

## Simplified Source

```c
static inline bool
SlruCorrectSegmentFilenameLength(SlruCtl ctl, size_t len)
{
    // Check if using modern long segment names (15 chars for 64-bit pages)
    if (ctl->long_segment_names)
        return (len == 15);

    // Legacy mode: support 4, 5, or 6 character filenames
    // (backward compatibility for older SLRU formats)
    return (len == 4 || len == 5 || len == 6);
}
```