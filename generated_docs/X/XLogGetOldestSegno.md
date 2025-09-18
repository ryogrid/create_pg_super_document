# XLogGetOldestSegno

## Location
[src/backend/access/transam/xlog.c:3751-3788](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L3751-L3788)

## Overview
XLogGetOldestSegno scans the WAL directory to find and return the oldest WAL segment number that exists for a specified timeline, or 0 if none are found.

## Definition
```c
XLogSegNo XLogGetOldestSegno(TimeLineID tli)
```

## Detailed Description
This function performs a filesystem-based discovery of the oldest WAL segment for a given timeline by scanning the XLOGDIR directory. It examines all files in the directory, filters for valid WAL segment files, extracts timeline and segment number information from filenames, and identifies the lowest segment number that matches the requested timeline. This approach provides an authoritative view of what WAL segments are actually present on disk, which is essential for operations that need to understand the complete range of available WAL data.

The function implements a straightforward directory scanning algorithm that parses WAL filenames to extract metadata and maintains running state to track the oldest segment encountered. It carefully validates filenames to ensure only genuine WAL segments are processed and ignores any files that don\t conform to expected naming patterns.