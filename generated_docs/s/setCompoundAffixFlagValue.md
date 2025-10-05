# setCompoundAffixFlagValue

## Location
[src/backend/tsearch/spell.c:1032-1067](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L1032-L1067)

## Overview
Sets the flag and value properties of a CompoundAffixFlag entry based on the dictionary's flag mode configuration.

## Definition
```c
static void setCompoundAffixFlagValue(IspellDict *Conf, CompoundAffixFlag *entry,
                                      char *s, uint32 val)
```

## Detailed Description
This function configures a CompoundAffixFlag entry by setting its flag representation and associated value. The flag can be stored in different formats depending on the dictionary's flagMode setting:

- For FM_NUM (numeric flag mode): Parses the string as an integer and validates it falls within the acceptable range (0 to FLAGNUM_MAXSIZE)
- For other flag modes: Stores the flag as a string using cpstrdup to create a copy in the dictionary's memory context

The function also sets the flagMode and value fields of the entry to match the provided parameters.

## Parameters / Member Variables
- `Conf`: Pointer to the IspellDict configuration containing flag mode settings
- `entry`: Pointer to the CompoundAffixFlag structure to be configured
- `s`: String representation of the flag to be set
- `val`: Numeric value to associate with this flag

## Dependencies
- Functions called/Symbols referenced:
  - strtol: Standard C library function for string to long conversion
  - ereport: PostgreSQL error reporting function
  - [cpstrdup](../c/cpstrdup.md): Dictionary-specific string duplication function
  - FM_NUM: Flag mode constant for numeric flags
  - FLAGNUM_MAXSIZE: Maximum allowed value for numeric flags
- Called from (representative examples):
  - [addCompoundAffixFlagValue](../a/addCompoundAffixFlagValue.md): Adds new compound affix flag entries
  - [getCompoundAffixFlagValue](../g/getCompoundAffixFlagValue.md): Retrieves compound affix flag values

## Notes and Other Information
- Validates numeric flags are within acceptable range and properly formatted
- Uses errno to detect overflow conditions during numeric parsing
- Memory for string flags is allocated in the dictionary's memory context
- Part of PostgreSQL's Hunspell-compatible spell checking implementation
- Supports different flag representation modes for compatibility with various dictionary formats

## Simplified Source

```c
static void setCompoundAffixFlagValue(IspellDict *Conf, CompoundAffixFlag *entry,
                                      char *s, uint32 val) {
    if (Conf->flagMode == FM_NUM) {
        // Parse string as numeric flag
        char *next;
        int i = strtol(s, &next, 10);

        // Validate numeric conversion
        if (s == next || errno == ERANGE)
            ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                          errmsg("invalid affix flag \"%s\"", s)));

        // Check range bounds
        if (i < 0 || i > FLAGNUM_MAXSIZE)
            ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
                          errmsg("affix flag \"%s\" is out of range", s)));

        entry->flag.i = i;
    } else {
        // Store flag as string for non-numeric modes
        entry->flag.s = cpstrdup(Conf, s);
    }

    // Set common properties
    entry->flagMode = Conf->flagMode;
    entry->value = val;
}
```