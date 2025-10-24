# writezone

## Location
[src/timezone/zic.c:2082-2437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L2082-L2437)

## Overview
The writezone function is responsible for generating and writing timezone data files in the PostgreSQL timezone compiler (zic), handling the complex process of optimizing timezone transitions and writing them in binary format.

## Definition

```c
struct tzhead tzh0;
```
## Detailed Description
The writezone function is the core output generator in PostgreSQL's timezone compiler. It takes timezone transition data that has been parsed and accumulated, optimizes it by removing redundant transitions, handles various compatibility requirements (including Qt bug workarounds), and writes the final timezone data file in the standard tzfile format.

The function operates in two passes:
1. Pass 1: Writes 32-bit compatible data for older systems
2. Pass 2: Writes 64-bit data for modern systems

Key optimizations include:
- Removing transitions that don't change the effective local time
- Merging consecutive transitions with identical timezone properties
- Handling leap second corrections
- Working around Qt bug QTBUG-53071 by inserting no-op transitions before 2038
- Reordering timezone types to optimize default type placement

## Parameters / Member Variables
- : The output filename for the timezone data file
- : The timezone rule string being processed
- : The tzfile format version to use
- : The default timezone type to use for times before any transitions

## Dependencies
- Functions called/Symbols referenced:
  - qsort (for sorting transitions)
  - [atcomp](../a/atcomp.md) (comparison function for transitions)
  - [emalloc](../e/emalloc.md) (memory allocation)
  - [limitrange](../l/limitrange.md) (to limit transition ranges for 32/64-bit output)
  - [tadd](../t/tadd.md) (time addition with overflow checking)
  - [addtype](../a/addtype.md) (to add new timezone types)
  - [want_bloat](want_bloat.md) (compatibility option checking)
  - [warning](warning.md) (for issuing warnings)
  - [mkdirs](../m/mkdirs.md) (directory creation)
  - fopen (file operations)
- Called from (representative examples):
  - [years_of_observations](../y/years_of_observations.md) (in src/timezone/zic.c:3341)

## Notes and Other Information
- The function handles both 32-bit and 64-bit timezone data formats
- Implements workarounds for various client bugs, particularly QTBUG-53071
- Warns when timezone files have more than 1200 transitions (compatibility issue)
- Performs extensive optimization to reduce file size and improve compatibility
- Handles leap second corrections when writing transition times
- Creates necessary directories if they don't exist
- The output format follows the standard tzfile specification used by Unix systems

## Simplified Source

```c
static void writezone(const char *const name, const char *const string,
                      char version, int defaulttype) {
    FILE *fp;
    ptrdiff_t i, j;
    int pass;
    static struct tzhead tzh;
    bool dir_checked = false;

    // Allocate arrays for transition times and types
    zic_t *ats = emalloc(MAXALIGN(size_product(nats, sizeof *ats + 1)));
    unsigned char *types = (unsigned char *)(ats + nats);
    struct timerange rangeall, range32, range64;

    // Sort transitions by time
    if (timecnt > 1)
        qsort(attypes, timecnt, sizeof *attypes, atcomp);

    // Optimize: remove redundant transitions
    ptrdiff_t toi = 0;
    for (ptrdiff_t fromi = 0; fromi < timecnt; ++fromi) {
        // Skip transitions that don't change effective local time
        if (should_merge_transition(fromi, toi)) {
            attypes[toi - 1].type = attypes[fromi].type;
            continue;
        }

        // Keep transition if it changes timezone properties
        if (transition_changes_properties(fromi, toi))
            attypes[toi++] = attypes[fromi];
    }
    timecnt = toi;

    // Transfer optimized data to output arrays
    for (i = 0; i < timecnt; ++i) {
        ats[i] = attypes[i].at;
        types[i] = attypes[i].type;
    }

    // Correct transition times for leap seconds
    for (i = 0; i < timecnt; ++i) {
        j = leapcnt;
        while (--j >= 0)
            if (ats[i] > trans[j] - corr[j]) {
                ats[i] = tadd(ats[i], corr[j]);
                break;
            }
    }

    // Work around Qt bug for 32-bit systems
    if (WORK_AROUND_QTBUG_53071 && timecnt != 0 &&
        ats[timecnt - 1] < y2038_boundary - 1) {
        ats[timecnt] = y2038_boundary - 1;
        types[timecnt] = types[timecnt - 1];
        timecnt++;
    }

    // Calculate data ranges for 32-bit and 64-bit formats
    rangeall.defaulttype = defaulttype;
    rangeall.base = rangeall.leapbase = 0;
    rangeall.count = timecnt;
    rangeall.leapcount = leapcnt;
    range64 = limitrange(rangeall, lo_time, hi_time, ats, types);
    range32 = limitrange(range64, PG_INT32_MIN, PG_INT32_MAX, ats, types);

    // Create output file
    if (remove(name) == 0)
        dir_checked = true;

    fp = fopen(name, "wb");
    if (!fp) {
        if (errno == ENOENT && !dir_checked) {
            mkdirs(name, true);
            fp = fopen(name, "wb");
        }
        if (!fp) {
            fprintf(stderr, "Cannot create %s/%s: %s\n",
                    progname, directory, name, strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    // Write data in two passes: 32-bit then 64-bit
    for (pass = 1; pass <= 2; ++pass) {
        struct timerange *range = (pass == 1) ? &range32 : &range64;

        // Set up type mappings and character strings
        setup_output_types(range, pass);

        // Write timezone header and data
        write_timezone_header(fp, range, pass);
        write_transition_data(fp, ats, types, range);
        write_timezone_types(fp, range);
        write_leap_seconds(fp, range);
        write_standard_indicators(fp, range);
        write_ut_indicators(fp, range);
    }

    fclose(fp);
}
```