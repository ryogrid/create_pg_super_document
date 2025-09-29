# makeObjectName

## Location
[src/backend/commands/indexcmds.c:2387-2474](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L2387-L2474)

## Overview
Creates appropriately sized and formatted names for implicitly created database objects like indexes, sequences, and constraints by combining base names with type labels.

## Definition

```c
struct the string using the chosen lengths */
	name = palloc(name1chars + name2chars + overhead + 1);
```
## Detailed Description
This utility function generates standardized names for database objects that are created implicitly by PostgreSQL, such as automatically generated indexes for constraints, sequences for serial columns, and other derived objects. The function implements a sophisticated truncation algorithm to ensure the resulting name fits within PostgreSQL's NAMEDATALEN limit while maintaining readability and uniqueness.

The naming pattern follows the format "name1_name2_label", where:
- name1 is typically the table name
- name2 is typically a column name (optional)
- label is a type identifier like "seq", "pkey", "idx" (optional)

When truncation is necessary due to length constraints, the function preferentially truncates the longer of name1 and name2, while never truncating the label portion. This ensures that the type identifier remains intact for disambiguation. The function also properly handles multibyte character boundaries to avoid breaking UTF-8 sequences.

## Parameters / Member Variables
- : Primary name component, typically a table name (required)
- : Secondary name component, typically a column name (optional, can be NULL)
- : Type identifier/suffix for the object type (optional, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_mbcliplen](../p/pg_mbcliplen.md) (for multibyte-safe string truncation)
  - [palloc](../p/palloc.md) (for memory allocation)
  - strlen, memcpy, strcpy (standard string operations)
  - NAMEDATALEN (PostgreSQL's name length limit constant)
- Called from (representative examples):
  - [ChooseRelationName](../C/ChooseRelationName.md) (for generating index names)
  - [ChooseConstraintName](../C/ChooseConstraintName.md) (for generating constraint names)
  - [ChooseExtendedStatisticName](../C/ChooseExtendedStatisticName.md) (for generating statistics object names)
  - [makeArrayTypeName](makeArrayTypeName.md) (for generating array type names)

## Notes and Other Information
- Returns a palloc'd string that the caller must eventually free
- The caller is responsible for checking uniqueness and potentially retrying with modified labels
- Never truncates the label portion, ensuring object type identification remains clear
- Uses multibyte-aware truncation to avoid breaking Unicode character sequences
- Implements a fair truncation algorithm that prefers shortening the longer name component
- Critical for maintaining consistent naming conventions across PostgreSQL's automatic object creation
- The function assumes the label is reasonably short since it's never truncated

## Simplified Source

```c
char *
makeObjectName(const char *name1, const char *name2, const char *label)
{
    char *name;
    int overhead = 0;    // chars needed for label and underscores
    int availchars;      // chars available for name(s)
    int name1chars, name2chars;

    // Calculate initial lengths
    name1chars = strlen(name1);
    if (name2) {
        name2chars = strlen(name2);
        overhead++;      // underscore separator
    } else {
        name2chars = 0;
    }
    if (label)
        overhead += strlen(label) + 1;  // label + underscore

    // Calculate available space for names
    availchars = NAMEDATALEN - 1 - overhead;
    Assert(availchars > 0);

    // Truncate names if necessary, preferring to shorten the longer one
    while (name1chars + name2chars > availchars) {
        if (name1chars > name2chars)
            name1chars--;
        else
            name2chars--;
    }

    // Ensure multibyte character boundaries are respected
    name1chars = pg_mbcliplen(name1, name1chars, name1chars);
    if (name2)
        name2chars = pg_mbcliplen(name2, name2chars, name2chars);

    // Construct the final name: "name1_name2_label"
    name = palloc(name1chars + name2chars + overhead + 1);
    int ndx = 0;

    // Copy name1
    memcpy(name, name1, name1chars);
    ndx = name1chars;

    // Add name2 if present
    if (name2) {
        name[ndx++] = '_';
        memcpy(name + ndx, name2, name2chars);
        ndx += name2chars;
    }

    // Add label if present
    if (label) {
        name[ndx++] = '_';
        strcpy(name + ndx, label);
    } else {
        name[ndx] = '\0';
    }

    return name;
}
```