# get_config_unit_name

## Location
[src/backend/utils/misc/guc.c:2816-2872](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2816-L2872)

## Overview
Returns the human-readable unit name string for a PostgreSQL configuration parameter based on its unit flags.

## Definition
```c
const char *get_config_unit_name(int flags)
```

## Detailed Description
This function maps PostgreSQL GUC (Grand Unified Configuration) unit flags to their corresponding string representations. It handles both memory units (bytes, kilobytes, megabytes, blocks) and time units (milliseconds, seconds, minutes). The function is essential for displaying configuration parameters with their appropriate units to users.

For block-based units (GUC_UNIT_BLOCKS and GUC_UNIT_XBLOCKS), the function dynamically generates unit strings based on the actual block sizes (BLCKSZ and XLOG_BLCKSZ respectively), using static buffers that are initialized on first use.

The function returns NULL for unitless parameters, allowing callers to distinguish between parameters that have units and those that don't.

## Parameters / Member Variables
- `flags`: Integer containing GUC unit flags that specify the type of unit for the configuration parameter

## Dependencies
- Functions called/Symbols referenced:
  - GUC_UNIT (bitmask constant)
  - GUC_UNIT_BYTE, GUC_UNIT_KB, GUC_UNIT_MB (memory unit constants)
  - GUC_UNIT_BLOCKS, GUC_UNIT_XBLOCKS (block unit constants)
  - GUC_UNIT_MS, GUC_UNIT_S, GUC_UNIT_MIN (time unit constants)
  - BLCKSZ, XLOG_BLCKSZ (block size constants)
  - snprintf (standard library function)
  - elog (PostgreSQL logging function)
- Called from (representative examples):
  - [parse_and_validate_value](../p/parse_and_validate_value.md)
  - [GetConfigOptionValues](../G/GetConfigOptionValues.md)
  - EmitWarningsOnPlaceholders

## Notes and Other Information
- Uses static buffers for dynamically generated unit strings (blocks and xlog blocks)
- Returns NULL for unitless GUC parameters (flags & GUC_UNIT == 0)
- Throws an ERROR if an unrecognized unit flag is encountered
- Block units are represented as "{size}kB" format (e.g., "8kB" for 8KB blocks)
- Part of PostgreSQL's configuration system infrastructure

## Simplified Source

```c
const char *
get_config_unit_name(int flags)
{
    switch (flags & GUC_UNIT) {
        case 0:
            return NULL;        // No units

        // Memory units
        case GUC_UNIT_BYTE:
            return "B";
        case GUC_UNIT_KB:
            return "kB";
        case GUC_UNIT_MB:
            return "MB";

        // Block units (dynamically sized)
        case GUC_UNIT_BLOCKS:
            {
                static char bbuf[8];
                if (bbuf[0] == '\0')  // Initialize once
                    snprintf(bbuf, sizeof(bbuf), "%dkB", BLCKSZ / 1024);
                return bbuf;
            }
        case GUC_UNIT_XBLOCKS:
            {
                static char xbuf[8];
                if (xbuf[0] == '\0')  // Initialize once
                    snprintf(xbuf, sizeof(xbuf), "%dkB", XLOG_BLCKSZ / 1024);
                return xbuf;
            }

        // Time units
        case GUC_UNIT_MS:
            return "ms";
        case GUC_UNIT_S:
            return "s";
        case GUC_UNIT_MIN:
            return "min";

        default:
            elog(ERROR, "unrecognized GUC units value: %d", flags & GUC_UNIT);
            return NULL;
    }
}
```