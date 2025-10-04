# check_datestyle

## Location
[src/backend/commands/variable.c:52-243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L52-L243)

## Overview
A GUC (Grand Unified Configuration) validation hook function that parses and validates datestyle configuration strings, ensuring they contain valid date style and order specifications.

## Definition

```c
structs like "DEFAULT, ISO".
			 */
			char	   *subval;
```
## Detailed Description
The  function serves as a GUC check hook that validates and processes the  configuration parameter in PostgreSQL. It parses comma-separated values to determine both the date output style (ISO, SQL, German, Postgres) and date order (YMD, DMY, MDY). The function performs comprehensive validation to ensure no conflicting specifications are provided and constructs a canonical string representation of the final configuration.

The function supports the following date styles:
- **ISO**: ISO 8601 standard format
- **SQL**: Traditional SQL format  
- **German**: German locale format (implies DMY order)
- **Postgres**: PostgreSQL traditional format

And the following date orders:
- **YMD**: Year-Month-Day
- **DMY**: Day-Month-Year (also accepts "EURO")
- **MDY**: Month-Day-Year (also accepts "US", "NONEURO")

The function also handles the special "DEFAULT" keyword by recursively parsing the system's default datestyle configuration.

## Parameters / Member Variables
- : Double pointer to the input configuration string that will be replaced with the canonical form upon successful validation
- : Double pointer that will contain additional data (int array with style and order values) for use by the assignment function
- : The source of the GUC setting (file, command line, etc.) - used for logging and validation context

## Dependencies
- Functions called/Symbols referenced:
  - [SplitIdentifierString](../S/SplitIdentifierString.md): Parses comma-separated configuration values
  - GUC_check_errdetail: Reports detailed error messages for GUC validation failures
  - [pg_strcasecmp](../p/pg_strcasecmp.md), pg_strncasecmp: Case-insensitive string comparison functions
  - [guc_malloc](../g/guc_malloc.md), guc_free, guc_strdup: GUC memory management functions
  - [GetConfigOptionResetString](../G/GetConfigOptionResetString.md): Retrieves the default value for recursive DEFAULT parsing
  - [list_free](../l/list_free.md): Frees linked list structures
  - USE_ISO_DATES, USE_SQL_DATES, USE_GERMAN_DATES, USE_POSTGRES_DATES: Date style constants
  - DATEORDER_YMD, DATEORDER_DMY, DATEORDER_MDY: Date order constants

- Called from (representative examples):
  - GUC system during configuration validation
  - Recursively calls itself when processing DEFAULT keyword

## Notes and Other Information
- The function implements conflict detection to prevent contradictory specifications like "ISO,SQL" or "YMD,DMY"
- German style automatically sets DMY order unless explicitly overridden
- Memory management follows GUC conventions with guc_malloc/guc_free functions
- The canonical output format is always "Style, Order" (e.g., "ISO, YMD")
- The extra data structure contains a 2-element integer array: [dateStyle, dateOrder]
- Error messages are provided through GUC_check_errdetail for user feedback

## Simplified Source

```c
bool
check_datestyle(char **newval, void **extra, GucSource source)
{
    int newDateStyle = DateStyle;
    int newDateOrder = DateOrder;
    bool have_style = false;
    bool have_order = false;
    bool ok = true;
    char *rawstring;
    int *myextra;
    char *result;
    List *elemlist;
    ListCell *l;

    // Parse comma-separated configuration values
    rawstring = pstrdup(*newval);
    if (!SplitIdentifierString(rawstring, ',', &elemlist))
    {
        GUC_check_errdetail("List syntax is invalid.");
        pfree(rawstring);
        list_free(elemlist);
        return false;
    }

    // Process each configuration token
    foreach(l, elemlist)
    {
        char *tok = (char *) lfirst(l);

        // Check date styles
        if (pg_strcasecmp(tok, "ISO") == 0)
        {
            if (have_style && newDateStyle != USE_ISO_DATES)
                ok = false;  // conflicting styles
            newDateStyle = USE_ISO_DATES;
            have_style = true;
        }
        else if (pg_strcasecmp(tok, "SQL") == 0)
        {
            if (have_style && newDateStyle != USE_SQL_DATES)
                ok = false;
            newDateStyle = USE_SQL_DATES;
            have_style = true;
        }
        else if (pg_strncasecmp(tok, "POSTGRES", 8) == 0)
        {
            if (have_style && newDateStyle != USE_POSTGRES_DATES)
                ok = false;
            newDateStyle = USE_POSTGRES_DATES;
            have_style = true;
        }
        else if (pg_strcasecmp(tok, "GERMAN") == 0)
        {
            if (have_style && newDateStyle != USE_GERMAN_DATES)
                ok = false;
            newDateStyle = USE_GERMAN_DATES;
            have_style = true;
            // GERMAN also sets DMY unless overridden
            if (!have_order)
                newDateOrder = DATEORDER_DMY;
        }
        // Check date orders
        else if (pg_strcasecmp(tok, "YMD") == 0)
        {
            if (have_order && newDateOrder != DATEORDER_YMD)
                ok = false;
            newDateOrder = DATEORDER_YMD;
            have_order = true;
        }
        else if (pg_strcasecmp(tok, "DMY") == 0 || pg_strncasecmp(tok, "EURO", 4) == 0)
        {
            if (have_order && newDateOrder != DATEORDER_DMY)
                ok = false;
            newDateOrder = DATEORDER_DMY;
            have_order = true;
        }
        else if (pg_strcasecmp(tok, "MDY") == 0 || pg_strcasecmp(tok, "US") == 0 ||
                 pg_strncasecmp(tok, "NONEURO", 7) == 0)
        {
            if (have_order && newDateOrder != DATEORDER_MDY)
                ok = false;
            newDateOrder = DATEORDER_MDY;
            have_order = true;
        }
        else if (pg_strcasecmp(tok, "DEFAULT") == 0)
        {
            // Recursively parse default configuration
            char *subval = guc_strdup(LOG, GetConfigOptionResetString("datestyle"));
            void *subextra = NULL;

            if (!subval || !check_datestyle(&subval, &subextra, source))
            {
                if (subval) guc_free(subval);
                ok = false;
                break;
            }

            myextra = (int *) subextra;
            if (!have_style)
                newDateStyle = myextra[0];
            if (!have_order)
                newDateOrder = myextra[1];
            guc_free(subval);
            guc_free(subextra);
        }
        else
        {
            GUC_check_errdetail("Unrecognized key word: \"%s\".", tok);
            pfree(rawstring);
            list_free(elemlist);
            return false;
        }
    }

    pfree(rawstring);
    list_free(elemlist);

    if (!ok)
    {
        GUC_check_errdetail("Conflicting \"datestyle\" specifications.");
        return false;
    }

    // Create canonical string representation
    result = (char *) guc_malloc(LOG, 32);
    if (!result)
        return false;

    // Format style and order strings
    switch (newDateStyle)
    {
        case USE_ISO_DATES:
            strcpy(result, "ISO");
            break;
        case USE_SQL_DATES:
            strcpy(result, "SQL");
            break;
        case USE_GERMAN_DATES:
            strcpy(result, "German");
            break;
        default:
            strcpy(result, "Postgres");
            break;
    }

    switch (newDateOrder)
    {
        case DATEORDER_YMD:
            strcat(result, ", YMD");
            break;
        case DATEORDER_DMY:
            strcat(result, ", DMY");
            break;
        default:
            strcat(result, ", MDY");
            break;
    }

    guc_free(*newval);
    *newval = result;

    // Set up extra data for assignment function
    myextra = (int *) guc_malloc(LOG, 2 * sizeof(int));
    if (!myextra)
        return false;
    myextra[0] = newDateStyle;
    myextra[1] = newDateOrder;
    *extra = (void *) myextra;

    return true;
}
```