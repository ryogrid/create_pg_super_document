# pg_perm_setlocale

## Location
[src/backend/utils/adt/pg_locale.c:213-315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L213-L315)

## Overview
A PostgreSQL wrapper around the standard setlocale() function that provides additional functionality for environment variable management and Windows-specific message locale handling.

## Definition

```c
char *
pg_perm_setlocale(int category, const char *locale)
```
## Detailed Description
This function wraps the libc setlocale() function with two key enhancements. First, when changing LC_CTYPE, it updates gettext's encoding for the current message domain, which is necessary for proper internationalization support especially on Windows where GNU gettext doesn't automatically track LC_CTYPE. Second, upon successful locale changes, it sets the corresponding LC_XXX environment variable to match the new setting, ensuring that subsequent setlocale(..., "") calls preserve the configuration made through this routine.

The function handles platform-specific differences, particularly for Windows where LC_MESSAGES doesn't work through the standard setlocale() call and requires special handling through environment variables and IsoLocaleName() conversion.

## Parameters / Member Variables
- : The locale category to change (LC_COLLATE, LC_CTYPE, LC_MESSAGES, LC_MONETARY, LC_NUMERIC, LC_TIME)
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
LC_ALL=: The locale string to set, or NULL to query current setting

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
  - 
  - 
  -  (Windows only)
  - 
- Called from (representative examples):
  -  (src/backend/main/main.c:305, 306)
  -  (src/backend/utils/adt/pg_locale.c:457)
  -  (src/backend/utils/init/postinit.c:411, 418)

## Notes and Other Information
- Returns the result of setlocale() on success, NULL on failure
- On Windows, LC_MESSAGES is handled specially since the standard setlocale() doesn't support it
- The function ensures message encoding is properly updated when LC_CTYPE changes
- Environment variables are set to preserve locale settings across process boundaries
- Uses LOCALE_NAME_BUFLEN sized buffer for saving LC_CTYPE results
- Critical for PostgreSQL's internationalization and localization support

## Simplified Source

```c
// Simplified version of pg_perm_setlocale
char *
pg_perm_setlocale(int category, const char *locale)
{
    char *result;
    const char *envvar;

    // Step 1: Call system setlocale (with Windows special handling)
#ifndef WIN32
    result = setlocale(category, locale);
#else
    // On Windows, LC_MESSAGES needs special handling
    if (category == LC_MESSAGES) {
        result = (char *) locale;
        if (locale == NULL || locale[0] == '\0')
            return result;
    } else {
        result = setlocale(category, locale);
    }
#endif

    // Step 2: Return immediately if setlocale failed
    if (result == NULL)
        return NULL;

    // Step 3: Update message encoding when LC_CTYPE changes
    if (category == LC_CTYPE) {
        static char saved_ctype[LOCALE_NAME_BUFLEN];

        // Save the result since it might be overwritten
        strlcpy(saved_ctype, result, sizeof(saved_ctype));
        result = saved_ctype;

        // Update message encoding for proper internationalization
#ifdef ENABLE_NLS
        SetMessageEncoding(pg_bind_textdomain_codeset(textdomain(NULL)));
#else
        SetMessageEncoding(GetDatabaseEncoding());
#endif
    }

    // Step 4: Map category to environment variable name
    switch (category) {
        case LC_COLLATE:   envvar = "LC_COLLATE"; break;
        case LC_CTYPE:     envvar = "LC_CTYPE"; break;
        case LC_MESSAGES:
            envvar = "LC_MESSAGES";
#ifdef WIN32
            // Convert to ISO locale name on Windows
            result = IsoLocaleName(locale);
            if (result == NULL)
                result = (char *) locale;
#endif
            break;
        case LC_MONETARY:  envvar = "LC_MONETARY"; break;
        case LC_NUMERIC:   envvar = "LC_NUMERIC"; break;
        case LC_TIME:      envvar = "LC_TIME"; break;
        default:
            elog(FATAL, "unrecognized LC category: %d", category);
            return NULL;
    }

    // Step 5: Set environment variable to preserve locale setting
    if (setenv(envvar, result, 1) != 0)
        return NULL;

    return result;
}
```

Key simplifications made:
- Consolidated platform-specific conditional compilation into clearer sections
- Simplified comments to focus on the main algorithm steps
- Preserved the essential locale setting, message encoding update, and environment variable management logic
- Maintained all critical error handling and return paths
- Removed detailed implementation comments while keeping functional descriptions
- Kept the complete switch statement logic as it's core to the function's purpose