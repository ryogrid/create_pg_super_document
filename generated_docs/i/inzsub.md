# inzsub

## Location
[src/timezone/zic.c:1567-1665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1567-L1665)

## Overview
Processes the detailed parsing and validation of timezone zone data fields for both initial zone lines and zone continuation lines in the zic compiler.

## Definition

```c
struct zone z;
```
## Detailed Description
The  function is the core zone processing function that handles the detailed parsing of timezone zone data. It processes both regular Zone lines and Zone continuation lines based on the  parameter. The function:

- Sets up field indices based on whether this is a continuation line (ZFC_* constants) or regular zone line (ZF_* constants)
- Validates the zone name for regular zones using namecheck()
- Parses standard offset using gethms()
- Validates abbreviation format strings (must contain %s or %z if %)
- Processes optional UNTIL fields for zone transitions
- Validates continuation line chronological ordering
- Adds the processed zone to the global zones array

The function returns true if there are UNTIL fields (indicating more zone data follows), false otherwise.

## Parameters / Member Variables
- : Array of string pointers containing the parsed zone fields from input
- : Number of fields provided in the fields array  
- : Boolean indicating if this is a zone continuation line (true) or initial zone line (false)

## Dependencies
- Functions called/Symbols referenced:
  - [namecheck](../n/namecheck.md) (validates zone names)
  - [ecpyalloc](../e/ecpyalloc.md) (allocates and copies strings)
  - [gethms](../g/gethms.md) (parses time offset strings)
  - strchr (string character search)
  - [rulesub](../r/rulesub.md) (processes UNTIL rule data)
  - [rpytime](../r/rpytime.md) (calculates time from rule)
  - [growalloc](../g/growalloc.md) (grows the zones array)
  - [error](../e/error.md)/warning (reporting functions)
- Called from (representative examples):
  - [inzone](inzone.md) (for initial zone lines)
  - [inzcont](inzcont.md) (for zone continuation lines)

## Notes and Other Information
- Uses different field index constants for continuation lines (ZFC_*) vs regular zones (ZF_*)
- Validates abbreviation format strings - only %s and %z are allowed with %
- Handles format specifier 'z' by converting to 's' for compatibility
- Maintains max_format_len global variable for format string length tracking
- Performs chronological validation for continuation lines to ensure proper ordering
- Returns whether the zone has UNTIL fields indicating more data follows
- Part of PostgreSQL's timezone data compilation system (zic)

## Simplified Source

```c
static bool
inzsub(char **fields, int nfields, bool iscont)
{
    static struct zone z;
    int i_stdoff, i_rule, i_format;
    int i_untilyear, i_untilmonth, i_untilday, i_untiltime;
    bool hasuntil;

    // Set field indices based on line type (continuation vs initial)
    if (iscont) {
        // Use continuation field indices (ZFC_*)
        i_stdoff = ZFC_STDOFF;
        i_rule = ZFC_RULE;
        i_format = ZFC_FORMAT;
        i_untilyear = ZFC_TILYEAR;
        i_untilmonth = ZFC_TILMONTH;
        i_untilday = ZFC_TILDAY;
        i_untiltime = ZFC_TILTIME;
        z.z_name = NULL;
    } else {
        // Validate zone name and use regular field indices (ZF_*)
        if (!namecheck(fields[ZF_NAME]))
            return false;
        i_stdoff = ZF_STDOFF;
        i_rule = ZF_RULE;
        i_format = ZF_FORMAT;
        i_untilyear = ZF_TILYEAR;
        i_untilmonth = ZF_TILMONTH;
        i_untilday = ZF_TILDAY;
        i_untiltime = ZF_TILTIME;
        z.z_name = ecpyalloc(fields[ZF_NAME]);
    }

    // Parse basic zone data
    z.z_filename = filename;
    z.z_linenum = linenum;
    z.z_stdoff = gethms(fields[i_stdoff], _("invalid UT offset"));

    // Validate abbreviation format (only %s and %z allowed)
    char *format_ptr = strchr(fields[i_format], '%');
    if (format_ptr != NULL) {
        if ((*(++format_ptr) != 's' && *format_ptr != 'z') ||
            strchr(format_ptr, '%') || strchr(fields[i_format], '/')) {
            error(_("invalid abbreviation format"));
            return false;
        }
    }

    // Set zone format and rule
    z.z_rule = ecpyalloc(fields[i_rule]);
    z.z_format = ecpyalloc(fields[i_format]);
    z.z_format_specifier = format_ptr ? *format_ptr : '\0';

    // Handle 'z' format specifier (convert to 's' for compatibility)
    if (z.z_format_specifier == 'z') {
        if (noise)
            warning(_("format '%s' not handled by pre-2015 versions of zic"), z.z_format);
        z.z_format[format_ptr - fields[i_format]] = 's';
    }

    // Track maximum format length
    if (max_format_len < strlen(z.z_format))
        max_format_len = strlen(z.z_format);

    // Process UNTIL fields if present
    hasuntil = nfields > i_untilyear;
    if (hasuntil) {
        // Parse until rule and calculate until time
        z.z_untilrule.r_filename = filename;
        z.z_untilrule.r_linenum = linenum;
        rulesub(&z.z_untilrule, fields[i_untilyear], "only", "",
                (nfields > i_untilmonth) ? fields[i_untilmonth] : "Jan",
                (nfields > i_untilday) ? fields[i_untilday] : "1",
                (nfields > i_untiltime) ? fields[i_untiltime] : "0");
        z.z_untiltime = rpytime(&z.z_untilrule, z.z_untilrule.r_loyear);

        // Validate chronological ordering for continuation lines
        if (iscont && nzones > 0 &&
            z.z_untiltime > min_time && z.z_untiltime < max_time &&
            zones[nzones - 1].z_untiltime > min_time &&
            zones[nzones - 1].z_untiltime < max_time &&
            zones[nzones - 1].z_untiltime >= z.z_untiltime) {
            error(_("Zone continuation line end time is not after end time of previous line"));
            return false;
        }
    }

    // Add zone to global zones array
    zones = growalloc(zones, sizeof *zones, nzones, &nzones_alloc);
    zones[nzones++] = z;

    // Return true if more zone data follows (UNTIL field present)
    return hasuntil;
}
```