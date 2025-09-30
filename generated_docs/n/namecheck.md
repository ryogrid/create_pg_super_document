# namecheck

## Location
[src/timezone/zic.c:903-950](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L903-L950)

## Overview
Validates timezone file names for portability and compliance with safe naming conventions in the PostgreSQL timezone compiler, checking for problematic characters and path components.

## Definition

```c
static bool
namecheck(const char *name)
```
## Detailed Description
The namecheck function performs comprehensive validation of timezone file names to ensure they are portable across different operating systems and filesystems. It validates both the overall filename and individual path components by:

1. **Character Validation**: Checks each character against a set of "benign" characters that are safe across all platforms (letters, numbers, hyphens, underscores, forward slashes)
2. **Warning Generation**: Issues warnings for characters that are printable but potentially problematic on some systems
3. **Component Validation**: Splits the path by '/' separators and validates each component using the componentcheck function
4. **Portability Enforcement**: Ensures generated timezone files will work correctly across diverse Unix and Unix-like systems

The function defines two character sets:
- : Safe characters including alphanumeric, hyphen, underscore, and slash
- : Printable characters that may cause issues on some systems

## Parameters / Member Variables
- : The timezone file name/path to validate (null-terminated string)

## Dependencies
- Functions called/Symbols referenced:
  - [warning](../w/warning.md) (issues warnings for potentially problematic characters)
  - componentcheck (validates individual path components)
  - HAVE_SYMLINK (conditional compilation symbol)
- Called from (representative examples):
  - [inzsub](../i/inzsub.md) (in src/timezone/zic.c:1592)
  - [inlink](../i/inlink.md) (in src/timezone/zic.c:1812)

## Notes and Other Information
- Returns true if the name passes validation, false if any component check fails
- Issues warnings (not errors) for non-benign but printable characters
- Critical for ensuring timezone data files are portable across different Unix systems
- Part of the timezone file creation and management infrastructure
- Handles both absolute and relative path validation
- Uses octal notation for non-printable character warnings

## Simplified Source

```c
static bool namecheck(const char *name) {
    // Define safe characters for portable filenames
    static char const benign[] = "-/_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static char const printable_and_not_benign[] = " !\"#$%&'()*+,.0123456789:;<=>?@[\\]^`{|}~";

    const char *component = name;

    // Check each character in the filename
    for (const char *cp = name; *cp; cp++) {
        unsigned char c = *cp;

        // Warn about non-benign characters if noise is enabled
        if (noise && !strchr(benign, c)) {
            warning((strchr(printable_and_not_benign, c)
                    ? _("file name '%s' contains byte '%c'")
                    : _("file name '%s' contains byte '\\%o'")),
                    name, c);
        }

        // Check path components at each '/' separator
        if (c == '/') {
            if (!componentcheck(name, component, cp))
                return false;
            component = cp + 1;
        }
    }

    // Check the final component
    return componentcheck(name, component, cp);
}
```