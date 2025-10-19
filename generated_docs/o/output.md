# output

## Location
[src/bin/pg_test_timing/pg_test_timing.c:182-208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_test_timing/pg_test_timing.c#L182-L208)

## Overview
The output function displays a formatted histogram of timing durations collected during the timing test, providing a statistical breakdown of clock resolution measurements.

## Definition

```c
static void
output(uint64 loop_count)
```
## Detailed Description
This function generates a comprehensive histogram report showing the distribution of timing measurements collected by the test_timing function. It analyzes the histogram array to find the highest significant bit position with non-zero values, then displays a formatted table showing timing ranges, percentages of total measurements, and raw counts.

The histogram uses a logarithmic scale where each bin represents timing durations in powers of 2 microseconds (< 1μs, < 2μs, < 4μs, etc.). This approach provides meaningful insights into system clock granularity and timing behavior. The output includes localized headers and uses proper formatting to align columns for readability.

## Parameters / Member Variables
- `loop_count`: Total number of timing measurements taken, used to calculate percentages
## Dependencies
- Functions called/Symbols referenced:
  - strlen (string length calculation)
  - printf (formatted output)
  - Max (macro for maximum value calculation)
- Called from:
  - [main](../m/main.md) (pg_test_timing.c:37)

## Dependencies on Global Variables
- : Global array containing timing measurement counts for each power-of-2 microsecond bin

## Notes and Other Information
- Uses internationalization support with gettext macros for localized output
- Automatically determines the maximum significant timing bin to avoid displaying empty ranges
- Provides percentage calculations to show relative frequency of different timing granularities
- Output format includes aligned columns with appropriate spacing for readability
- Function is static, indicating it's only used within the same compilation unit
- Critical for interpreting timing test results and understanding system clock characteristics

## Simplified Source

```c
static void
output(uint64 loop_count)
{
    int64 max_bit = 31, i;

    // Set up localized headers
    char *header1 = _("< us");
    char *header2 = _("% of total");
    char *header3 = _("count");
    int len1 = strlen(header1);
    int len2 = strlen(header2);
    int len3 = strlen(header3);

    // Find highest non-empty histogram bin
    while (max_bit > 0 && histogram[max_bit] == 0)
        max_bit--;

    // Print histogram header
    printf(_("Histogram of timing durations:\n"));
    printf("%*s   %*s %*s\n",
           Max(6, len1), header1,
           Max(10, len2), header2,
           Max(10, len3), header3);

    // Print histogram data
    for (i = 0; i <= max_bit; i++)
        printf("%*ld    %*.5f %*lld\n",
               Max(6, len1), 1l << i,
               Max(10, len2) - 1, (double) histogram[i] * 100 / loop_count,
               Max(10, len3), histogram[i]);
}
```