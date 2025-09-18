# SlruFileName

## Location
[src/backend/access/transam/slru.c:91-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L91-L123)

## Overview
SlruFileName is a static inline function that converts a segment number to the corresponding filename for SLRU (Simple LRU) segments, supporting both standard and long segment name formats.

## Definition


## Detailed Description
This function generates SLRU segment filenames based on the segment number and the control structure's configuration. It supports two naming schemes:

1. **Long segment names** (when ctl->long_segment_names is true): Creates 15-character hexadecimal filenames for segment numbers in the range [0, 2^60-1]. The format is "dir/123456789ABCDEF". The 15-character limit is intentionally chosen to distinguish SLRU segments from WAL segments.

2. **Standard segment names** (when ctl->long_segment_names is false): Creates 4-6 character hexadecimal filenames for segment numbers in the range [0, 2^24-1]. The format varies based on the segment number:
   - "dir/1234" for [0, 2^16-1]  
   - "dir/12345" for [2^16, 2^20-1]
   - "dir/123456" for [2^20, 2^24-1]

The function uses snprintf to safely construct the path string and returns the number of characters written.

## Parameters / Member Variables
- : SlruCtl structure containing SLRU control information, including the directory path (ctl->Dir) and naming configuration (ctl->long_segment_names)
- : Character buffer to store the resulting filename path (should be at least MAXPGPATH characters long)
- : 64-bit segment number to convert to filename

## Dependencies
- Functions called/Symbols referenced:
  - SlruCtl (structure type)
  - INT64CONST (macro for 64-bit constants)
  - snprintf (standard C library function)
  - Assert (debugging macro)
  - MAXPGPATH (constant defining maximum path length)

- Called from (representative examples):
  - [SimpleLruDoesPhysicalPageExist](SimpleLruDoesPhysicalPageExist.md)
  - [SlruPhysicalReadPage](SlruPhysicalReadPage.md)
  - [SlruPhysicalWritePage](SlruPhysicalWritePage.md)
  - [SlruReportIOError](SlruReportIOError.md)
  - [SlruInternalDeleteSegment](SlruInternalDeleteSegment.md)
  - [SlruSyncFileTag](SlruSyncFileTag.md)

## Notes and Other Information
- The function is marked as static inline for performance optimization since it's frequently called
- The 15-character limit for long segment names is a deliberate design choice to avoid confusion with WAL segments
- The function includes range assertions to ensure segment numbers are within valid bounds for each naming scheme
- The %04X format string is used for standard names but supports up to 24-bit integers through SlruCorrectSegmentFilenameLength()
- This function is central to SLRU file management and is used throughout the SLRU subsystem for file operations