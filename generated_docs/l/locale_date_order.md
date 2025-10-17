# locale_date_order

## Location
[src/bin/initdb/initdb.c:2125-2183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2125-L2183)

## Overview
Determines the likely date order format (MDY, DMY, or YMD) from a given locale by testing the locale's date formatting behavior.

## Definition

```c
struct tm	testtime;
```
## Detailed Description
This function determines the date ordering convention used by a specific locale by temporarily setting the system locale and formatting a test date. It creates a test date (November 22, 2033) and formats it using the locale's "%x" format specifier, then analyzes the positions of the day (22), month (11), and year (33) components in the resulting string to determine whether the locale uses Month-Day-Year (MDY), Day-Month-Year (DMY), or Year-Month-Day (YMD) ordering.

The function is used during PostgreSQL database initialization to configure appropriate date formatting based on the system locale. It employs a safe locale switching mechanism that saves and restores the original LC_TIME locale setting to avoid side effects.

## Parameters / Member Variables

LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL=: A string specifying the locale name to test for date ordering (e.g., "en_US", "de_DE")

## Dependencies
- Functions called/Symbols referenced:
  - [save_global_locale](../s/save_global_locale.md) (saves current locale state)
  - setlocale (sets the LC_TIME locale temporarily)
  - [my_strftime](../m/my_strftime.md) (formats the test date)
  - [restore_global_locale](../r/restore_global_locale.md) (restores original locale state)
  - DATEORDER_MDY, DATEORDER_DMY, DATEORDER_YMD (constants for date order types)
- Called from (representative examples):
  - [setup_config](../s/setup_config.md) (during database initialization configuration)

## Notes and Other Information
- Returns DATEORDER_MDY as the default if locale formatting fails or produces unexpected results
- Uses a carefully chosen test date (November 22, 2033) to ensure clear distinction between day, month, and year components
- The function is locale-safe, preserving the original LC_TIME setting
- Part of the initdb utility used during PostgreSQL database cluster initialization
- The function handles edge cases where strftime fails or produces malformed output gracefully

## Simplified Source

```c
static int locale_date_order(const char *locale) {
    struct tm testtime;
    char buf[128];
    char *posD, *posM, *posY;
    save_locale_t save;
    int result = DATEORDER_MDY;  // default

    // Save current locale and set to test locale
    save = save_global_locale(LC_TIME);
    setlocale(LC_TIME, locale);

    // Create test date: November 22, 2033
    memset(&testtime, 0, sizeof(testtime));
    testtime.tm_mday = 22;    // Day: 22
    testtime.tm_mon = 10;     // Month: November (11)
    testtime.tm_year = 133;   // Year: 2033

    // Format date and analyze component positions
    if (my_strftime(buf, sizeof(buf), "%x", &testtime) > 0) {
        posM = strstr(buf, "11");  // Find month
        posD = strstr(buf, "22");  // Find day
        posY = strstr(buf, "33");  // Find year

        if (posM && posD && posY) {
            if (posY < posM && posM < posD)
                result = DATEORDER_YMD;
            else if (posD < posM)
                result = DATEORDER_DMY;
            else
                result = DATEORDER_MDY;
        }
    }

    // Restore original locale
    restore_global_locale(LC_TIME, save);
    return result;
}
```