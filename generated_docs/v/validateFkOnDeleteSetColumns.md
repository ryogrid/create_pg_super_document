# validateFkOnDeleteSetColumns

## Location
[src/backend/commands/tablecmds.c:10048-10122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L10048-L10122)

## Overview
Validates that columns specified in ON DELETE SET NULL/DEFAULT (...) column lists are valid foreign key columns and removes any duplicate entries from the list.

## Definition

```c
static int
validateFkOnDeleteSetColumns(int numfks, const int16 *fkattnums,
							 int numfksetcols, int16 *fksetcolsattnums,
							 List *fksetcols)
```
## Detailed Description
This function performs validation on columns that are specified in ON DELETE SET NULL or ON DELETE SET DEFAULT actions with explicit column lists. It ensures that:

1. Each specified column is actually part of the foreign key constraint (exists in the fkattnums array)
2. Removes duplicate column references silently by compacting the fksetcolsattnums array
3. Returns the new count of unique columns after deduplication

The function validates each column in the fksetcolsattnums array against the foreign key columns in fkattnums. If a column is not found in the foreign key, it reports an error. For valid columns, it checks for duplicates and only keeps unique entries in the output array.

## Parameters / Member Variables
- : Number of columns in the foreign key constraint
- : Array of attribute numbers representing the foreign key columns
- : Initial count of columns in the SET action list
- : Array of attribute numbers for SET action columns (modified in-place to remove duplicates)
- : List of column names for error reporting purposes

## Dependencies
- Functions called/Symbols referenced:
  - [list_nth](../l/list_nth.md)
  - strVal
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - [ATAddForeignKeyConstraint](../A/ATAddForeignKeyConstraint.md)
  - child_dependency_type

## Notes and Other Information
- This is a static function within tablecmds.c, used internally for foreign key constraint processing
- The function modifies the fksetcolsattnums array in-place to remove duplicates
- Error reporting includes the actual column name from the fksetcols list for user-friendly error messages
- Returns the deduplicated count of columns, which may be less than the input numfksetcols if duplicates were found
- Part of PostgreSQL's ALTER TABLE foreign key constraint validation infrastructure

## Simplified Source

```c
static int
validateFkOnDeleteSetColumns(int numfks, const int16 *fkattnums,
                            int numfksetcols, int16 *fksetcolsattnums,
                            List *fksetcols)
{
    int numcolsout = 0;

    for (int i = 0; i < numfksetcols; i++)
    {
        int16 setcol_attnum = fksetcolsattnums[i];
        bool seen = false;

        // Verify column is part of foreign key
        for (int j = 0; j < numfks; j++)
        {
            if (fkattnums[j] == setcol_attnum)
            {
                seen = true;
                break;
            }
        }

        if (!seen)
        {
            char *col = strVal(list_nth(fksetcols, i));
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                     errmsg("column \"%s\" referenced in ON DELETE SET action must be part of foreign key", col)));
        }

        // Check for duplicates and deduplicate
        seen = false;
        for (int j = 0; j < numcolsout; j++)
        {
            if (fksetcolsattnums[j] == setcol_attnum)
            {
                seen = true;
                break;
            }
        }

        if (!seen)
            fksetcolsattnums[numcolsout++] = setcol_attnum;
    }

    return numcolsout;  // Return deduplicated count
}
```