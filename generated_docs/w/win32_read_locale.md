# win32_read_locale

## Location
[src/backend/commands/collationcmds.c:778-839](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/collationcmds.c#L778-L839)

## Overview
A Windows callback function used by EnumSystemLocalesEx() to create PostgreSQL collations for each available system locale, including POSIX-style aliases.

## Definition
```c
static BOOL CALLBACK win32_read_locale(LPWSTR pStr, DWORD dwFlags, LPARAM lparam)
```

## Detailed Description
This function serves as a callback for the Windows API function EnumSystemLocalesEx(), which enumerates all available system locales on Windows. For each locale, it converts the wide-character locale name to a multibyte string, validates it, and attempts to create a PostgreSQL collation using create_collation_from_locale(). Additionally, it creates POSIX-style aliases by converting Windows-style hyphens to underscores (e.g., "en-US" becomes "en_US"). The function always returns TRUE to continue enumeration, handling errors gracefully by skipping invalid or problematic locales. This is a Windows-specific implementation used only when ENUM_SYSTEM_LOCALE is defined.

## Parameters / Member Variables
- `pStr`: Wide-character string containing the locale name from Windows
- `dwFlags`: Flags from EnumSystemLocalesEx (unused, marked with (void) cast)
- `lparam`: User-defined parameter cast to CollParam* containing namespace ID and counters

## Dependencies
- Functions called/Symbols referenced:
  - WideCharToMultiByte (Windows API)
  - GetLastError (Windows API)
  - create_collation_from_locale
  - strchr (C library)
  - strcpy (C library)
  - [CollationCreate](../C/CollationCreate.md)
  - [GetUserId](../G/GetUserId.md)
  - [get_collation_actual_version](../g/get_collation_actual_version.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - CollParam (struct type)
  - NAMEDATALEN (constant)
- Called from (representative examples):
  - [pg_import_system_collations](../p/pg_import_system_collations.md) (via EnumSystemLocalesEx callback)

## Notes and Other Information
- This is a Windows-specific function, only compiled when ENUM_SYSTEM_LOCALE is defined
- Uses the CALLBACK calling convention required by Windows API callback functions
- Converts Windows locale names (with hyphens) to POSIX-style names (with underscores) for compatibility
- Handles buffer overflow gracefully by skipping locales with names too long for NAMEDATALEN
- Always returns TRUE to continue enumeration, never stopping the process early
- Part of PostgreSQL's platform-specific collation import system for Windows
- Creates both the original Windows-style collation and a POSIX-style alias when hyphens are present

## Simplified Source

```c
static BOOL CALLBACK
win32_read_locale(LPWSTR pStr, DWORD dwFlags, LPARAM lparam)
{
    CollParam *param = (CollParam *) lparam;
    char localebuf[NAMEDATALEN];
    int result;
    int enc;

    // Convert wide-character locale name to multibyte string
    result = WideCharToMultiByte(CP_ACP, 0, pStr, -1, localebuf, NAMEDATALEN, NULL, NULL);

    // Skip if conversion failed or name too long
    if (result == 0 || localebuf[0] == '\0')
        return TRUE;

    // Create collation from locale
    enc = create_collation_from_locale(localebuf, param->nspid, param->nvalidp, param->ncreatedp);
    if (enc < 0)
        return TRUE;

    // Create POSIX alias if locale contains hyphens (convert "en-US" to "en_US")
    if (strchr(localebuf, '-'))
    {
        char alias[NAMEDATALEN];
        Oid collid;

        strcpy(alias, localebuf);
        // Replace hyphens with underscores
        for (char *p = alias; *p; p++)
            if (*p == '-')
                *p = '_';

        // Create the POSIX-style collation alias
        collid = CollationCreate(alias, param->nspid, GetUserId(),
                                COLLPROVIDER_LIBC, true, enc,
                                localebuf, localebuf, NULL, NULL,
                                get_collation_actual_version(COLLPROVIDER_LIBC, localebuf),
                                true, true);
        if (OidIsValid(collid))
        {
            (*param->ncreatedp)++;
            CommandCounterIncrement();
        }
    }

    return TRUE; // Continue enumeration
}
```