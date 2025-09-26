# DynamicZoneAbbrev

## Location
[src/include/utils/datetime.h:224-228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/datetime.h#L224-L228)

## Overview
DynamicZoneAbbrev is a structure that stores auxiliary data for dynamic time zone abbreviations that do not have fixed offsets, enabling PostgreSQL to handle timezone abbreviations that vary based on context and location.

## Definition

Source: src/include/utils/datetime.h:223-228

## Detailed Description
DynamicZoneAbbrev provides support for timezone abbreviations that cannot be resolved to a fixed offset from UTC. Unlike static timezone abbreviations that always represent the same offset (like 'PST' always being UTC-8), dynamic abbreviations may represent different offsets depending on the date due to daylight saving time transitions or historical timezone changes. The structure uses lazy evaluation - the pg_tz pointer starts as NULL and is populated when the timezone information is first needed. This design optimizes memory usage and startup performance by deferring expensive timezone lookups until actually required.

## Parameters / Member Variables
- : Pointer to a pg_tz structure containing the resolved timezone information; initially NULL until the timezone is looked up
- : Flexible array member storing the NUL-terminated timezone name string that identifies the dynamic timezone

## Dependencies
- Functions called/Symbols referenced:
  - [pg_tz](../p/pg_tz.md) (opaque timezone structure type)
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array implementation)

- Called from (representative examples):
  - [ConvertTimeZoneAbbrevs](../C/ConvertTimeZoneAbbrevs.md) (src/backend/utils/adt/datetime.c:4892,4920,4923,4931)
  - [FetchDynamicTimeZone](../F/FetchDynamicTimeZone.md) (src/backend/utils/adt/datetime.c:4973,4979)

## Notes and Other Information
- This structure is used in conjunction with TimeZoneAbbrevTable to handle timezone abbreviations that require dynamic resolution
- The lazy evaluation approach (NULL tz pointer initially) helps avoid expensive timezone database lookups during system initialization
- Dynamic timezone abbreviations are necessary for handling cases where the same abbreviation might represent different UTC offsets depending on the date (e.g., due to daylight saving time rules)
- The flexible array member design allows efficient storage of variable-length timezone names within the same memory allocation
- This is part of PostgreSQL's comprehensive timezone handling system that supports both fixed and variable timezone abbreviations