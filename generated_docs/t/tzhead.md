# tzhead

## Location
[src/timezone/tzfile.h:39-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/tzfile.h#L39-L99)

## Overview
The tzhead structure represents the header portion of TZif (time zone information) files, containing metadata about timezone data format and counts for various timezone components.

## Definition
```c
struct tzhead
{
    char        tzh_magic[4];       /* TZ_MAGIC */
    char        tzh_version[1];     /* '0' or '2' or '3' as of 2013 */
    char        tzh_reserved[15];   /* reserved; must be zero */
    char        tzh_ttisutcnt[4];   /* coded number of trans. time flags */
    char        tzh_ttisstdcnt[4];  /* coded number of trans. time flags */
    char        tzh_leapcnt[4];     /* coded number of leap seconds */
    char        tzh_timecnt[4];     /* coded number of transition times */
    char        tzh_typecnt[4];     /* coded number of local time types */
    char        tzh_charcnt[4];     /* coded number of abbr. chars */
};
```

## Detailed Description
The tzhead structure defines the binary format header for TZif timezone files as specified in Internet RFC 8536. This header appears at the beginning of timezone data files and provides essential metadata about the timezone information that follows. All numeric fields are stored as character arrays in network byte order (big-endian) and must be decoded before use.

The structure supports multiple format versions, with version '2' and '3' files containing extended data sections with 8-byte transition times instead of 4-byte times, allowing representation of dates beyond the year 2038 limit.

## Parameters / Member Variables
- `tzh_magic[4]`: Magic number identifying the file as a TZif file (contains "TZif")
- `tzh_version[1]`: Format version - '0' for original format, '2' or '3' for extended formats supporting 64-bit transition times
- `tzh_reserved[15]`: Reserved space that must be filled with zeros for future compatibility
- `tzh_ttisutcnt[4]`: Count of UTC/local indicators for transition time types (network byte order)
- `tzh_ttisstdcnt[4]`: Count of standard/wall indicators for transition time types (network byte order)  
- `tzh_leapcnt[4]`: Number of leap second correction entries in the file (network byte order)
- `tzh_timecnt[4]`: Number of transition time entries in the file (network byte order)
- `tzh_typecnt[4]`: Number of local time type entries in the file (network byte order)
- `tzh_charcnt[4]`: Number of characters in the timezone abbreviation strings (network byte order)

## Dependencies
- Functions called/Symbols referenced:
  - TZ_MAGIC (constant defining "TZif" magic bytes)
- Called from (representative examples):
  - input_buffer (src/timezone/localtime.c:181, 184)
  - tzloadbody (src/timezone/localtime.c:219, 249-256, 410)
  - writezone (src/timezone/zic.c:2089, 2090)

## Notes and Other Information
- This structure is part of the public domain timezone code originally developed by Arthur David Olson
- All count fields use 4-byte network byte order encoding and must be converted to host byte order before use
- The structure is followed by variable-length data sections containing the actual timezone transition times, types, and abbreviations
- Version 2 and 3 formats include duplicate data sections with 8-byte transition times for extended date range support
- Maximum limits are enforced: 2000 transition times, 256 time types, 50 abbreviation characters, and 50 leap seconds
- The header format is designed for cross-platform compatibility and follows Internet RFC 8536 specifications