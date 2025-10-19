# identify_system_timezone

## Location
[src/bin/initdb/findtimezone.c:1565-1727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/findtimezone.c#L1565-L1727)

## Overview
Identifies the system timezone on Windows by matching the current timezone name against a predefined mapping table, with fallback support for localized timezone names via Windows registry lookup.

## Definition

```c
struct tm  *tm = localtime(&t);
```
## Detailed Description
This Windows-specific function implements a two-phase approach to identify the system timezone:

1. **Direct Name Matching**: Uses  to get the current timezone abbreviation (%Z) and matches it against the  table containing standard and daylight saving time names.

2. **Registry-Based Localized Lookup**: If direct matching fails (common with localized Windows versions), scans the Windows registry under "SOFTWARE\Microsoft\Windows NT\CurrentVersion\Time Zones" to find the English timezone name corresponding to the localized name, then matches against the mapping table again.

The function handles both standard time and daylight saving time abbreviations, and includes comprehensive error handling for registry operations. Returns NULL if no match is found, indicating the system should fall back to GMT.

## Parameters / Member Variables
- Returns:  - PostgreSQL timezone name from win32_tzmap, or NULL if no match found

## Dependencies
- Functions called/Symbols referenced:
  - time: Get current time
  - localtime: Convert time to local time structure
  - strftime: Format time string to get timezone abbreviation
  - win32_tzmap: Static mapping table of Windows to PostgreSQL timezone names
  - RegOpenKeyEx: Open Windows registry key
  - RegEnumKeyEx: Enumerate registry subkeys
  - RegQueryValueEx: Query registry value
  - RegCloseKey: Close registry handle
- Called from:
  - [select_default_timezone](../s/select_default_timezone.md): Main timezone selection function

## Notes and Other Information
- Windows-specific implementation (uses Windows Registry API)
- Handles localization issues where Windows returns timezone names in local language
- Includes extensive debug output when DEBUG_IDENTIFY_TIMEZONE is defined
- Part of PostgreSQL's initdb timezone auto-detection system
- Falls back to GMT (returns NULL) when timezone cannot be identified
- Uses win32_tzmap table that maps Windows timezone names to PostgreSQL timezone names
- Supports both standard time and daylight saving time name matching
- Registry scan is performed as fallback for localized Windows installations

## Simplified Source

```c
static const char *
identify_system_timezone(void)
{
    char tzname[128];
    char localtzname[256];
    time_t t = time(NULL);
    struct tm *tm = localtime(&t);

    if (!tm)
        return NULL;  // Fall back to GMT

    // Get current timezone abbreviation
    strftime(tzname, sizeof(tzname) - 1, "%Z", tm);

    // Phase 1: Direct match against win32_tzmap
    for (int i = 0; win32_tzmap[i].stdname != NULL; i++)
    {
        if (strcmp(tzname, win32_tzmap[i].stdname) == 0 ||
            strcmp(tzname, win32_tzmap[i].dstname) == 0)
        {
            return win32_tzmap[i].pgtzname;
        }
    }

    // Phase 2: Registry lookup for localized timezone names
    HKEY rootKey;
    if (RegOpenKeyEx(HKEY_LOCAL_MACHINE,
                     "SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\Time Zones",
                     0, KEY_READ, &rootKey) != ERROR_SUCCESS)
        return NULL;

    // Scan registry entries to find matching timezone
    for (int idx = 0; ; idx++)
    {
        char keyname[256];
        char zonename[256];
        DWORD namesize = sizeof(keyname);

        // Enumerate registry keys
        if (RegEnumKeyEx(rootKey, idx, keyname, &namesize,
                        NULL, NULL, NULL, NULL) != ERROR_SUCCESS)
            break;

        HKEY key;
        if (RegOpenKeyEx(rootKey, keyname, 0, KEY_READ, &key) != ERROR_SUCCESS)
            continue;

        // Check standard time name
        namesize = sizeof(zonename);
        if (RegQueryValueEx(key, "Std", NULL, NULL,
                           (unsigned char *) zonename, &namesize) == ERROR_SUCCESS &&
            strcmp(tzname, zonename) == 0)
        {
            strcpy(localtzname, keyname);
            RegCloseKey(key);
            break;
        }

        // Check daylight time name
        namesize = sizeof(zonename);
        if (RegQueryValueEx(key, "Dlt", NULL, NULL,
                           (unsigned char *) zonename, &namesize) == ERROR_SUCCESS &&
            strcmp(tzname, zonename) == 0)
        {
            strcpy(localtzname, keyname);
            RegCloseKey(key);
            break;
        }

        RegCloseKey(key);
    }

    RegCloseKey(rootKey);

    // Match localized name against win32_tzmap
    if (localtzname[0])
    {
        for (int i = 0; win32_tzmap[i].stdname != NULL; i++)
        {
            if (strcmp(localtzname, win32_tzmap[i].stdname) == 0 ||
                strcmp(localtzname, win32_tzmap[i].dstname) == 0)
            {
                return win32_tzmap[i].pgtzname;
            }
        }
    }

    return NULL;  // No match found, fall back to GMT
}
```