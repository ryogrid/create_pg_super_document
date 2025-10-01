# textregexreplace

## Location
[src/backend/utils/adt/regexp.c:658-698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L658-L698)

## Overview
Performs regular expression-based text replacement with pattern matching flags, taking a source text, regex pattern, replacement text, and option flags as input.

## Definition

```c
Datum
textregexreplace(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the PostgreSQL  SQL function with four arguments: source text, pattern, replacement, and flags. It provides pattern-based text substitution using regular expressions with configurable matching options.

The function includes special validation logic to handle ambiguous function resolution. When the fourth argument appears to be numeric (starts with '0'-'9'), it raises an error with a helpful hint suggesting the user might have intended to use the extended version with a start parameter instead.

After parsing the flags, it delegates the actual replacement work to  with appropriate parameters for single or global replacement based on the flags.

## Parameters / Member Variables
-  (text*): Source text to search within
-  (text*): Regular expression pattern to match
-  (text*): Replacement text to substitute matches
-  (text*): Option flags string controlling regex behavior (e.g., 'g' for global, 'i' for case-insensitive)

## Dependencies
- Functions called/Symbols referenced:
  -  (struct type for regex flags)
  -  (multibyte character length function)
  -  (parses option string into flags structure)
  -  (performs the actual regex replacement)
  -  (macro to return text result)
  -  (macro to get current collation)
- Called from (representative examples):
  -  (src/backend/commands/extension.c:1018)

## Notes and Other Information
- This function is the entry point for the 4-argument form of  SQL function
- Contains disambiguation logic to prevent confusion with the extended 5-argument version that takes a start position
- Uses PostgreSQL's standard function argument handling macros (PG_GETARG_TEXT_PP)
- Supports both single and global replacement modes based on the 'g' flag
- Part of PostgreSQL's regular expression functionality in the regexp.c module

## Simplified Source

```c
Datum textregexreplace(PG_FUNCTION_ARGS) {
    text *source = PG_GETARG_TEXT_PP(0);
    text *pattern = PG_GETARG_TEXT_PP(1);
    text *replacement = PG_GETARG_TEXT_PP(2);
    text *options = PG_GETARG_TEXT_PP(3);

    // Check for numeric flag (user confusion detection)
    if (VARSIZE_ANY_EXHDR(options) > 0) {
        char *opt_p = VARDATA_ANY(options);
        if (*opt_p >= '0' && *opt_p <= '9') {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid regular expression option: \"%.*s\"",
                        pg_mblen(opt_p), opt_p),
                 errhint("If you meant to use regexp_replace() with a start parameter, cast the fourth argument to integer explicitly.")));
        }
    }

    // Parse regex flags from options string
    pg_re_flags flags;
    parse_re_flags(&flags, options);

    // Perform replacement (single or global based on flags)
    PG_RETURN_TEXT_P(replace_text_regexp(source, pattern, replacement,
                                        flags.cflags, PG_GET_COLLATION(),
                                        0, flags.glob ? 0 : 1));
}
```