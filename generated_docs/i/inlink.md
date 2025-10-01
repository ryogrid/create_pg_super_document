# inlink

## Location
[src/timezone/zic.c:1798-1822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1798-L1822)

## Overview
Processes timezone link entries from database files, creating symbolic links between timezone names and their target zones.

## Definition

```c
struct link l;
```
## Detailed Description
The  function is part of PostgreSQL's timezone compiler () that handles "Link" lines from timezone database files. These lines create aliases or symbolic links from one timezone name to another, allowing multiple names to refer to the same timezone data.

The function performs validation to ensure the correct number of fields are present, verifies that the target field is not blank, validates the link name using Usage: /usr/bin/namecheck name, and creates a link structure containing the source file information, target timezone name, and link name.

The link information is stored in a dynamically growing array () that will be processed later to create the actual timezone links in the compiled timezone database.

## Parameters / Member Variables
- : Array of string fields parsed from the link line in the timezone database file
- : Number of fields in the fields array, must match LINK_FIELDS for valid link entries

## Dependencies
- Functions called/Symbols referenced:
  - [error](../e/error.md) (for reporting parsing errors)
  - [namecheck](../n/namecheck.md) (to validate the link name format)
  - [ecpyalloc](../e/ecpyalloc.md) (to allocate and copy target and linkname strings)
  - [growalloc](../g/growalloc.md) (to expand the links array as needed)
  - LINK_FIELDS (constant defining expected number of fields)
  - LF_TARGET, LF_LINKNAME (field index constants)
  - struct link (structure type for storing link information)
  - links, nlinks, nlinks_alloc (global variables for link storage)
  - filename, linenum (global variables for source tracking)
- Called from (representative examples):
  - [infile](infile.md) (main file parsing function)

## Notes and Other Information
- This function is part of PostgreSQL's timezone data compilation system, not the runtime timezone handling
- Links create aliases between timezone names - for example, linking "US/Eastern" to "America/New_York"
- The function stores link information for later processing rather than immediately creating the links
- Proper validation ensures that malformed link entries don't corrupt the timezone database
- The links array grows dynamically using  to accommodate any number of timezone links
- Each link structure contains source file tracking information for debugging and error reporting

## Simplified Source

```c
static void
inlink(char **fields, int nfields)
{
    // Validate field count
    if (nfields != LINK_FIELDS) {
        error(_("wrong number of fields on Link line"));
        return;
    }

    // Validate target field is not empty
    if (*fields[LF_TARGET] == '\0') {
        error(_("blank TARGET field on Link line"));
        return;
    }

    // Validate link name format
    if (!namecheck(fields[LF_LINKNAME]))
        return;

    // Create link structure
    struct link l;
    l.l_filename = filename;
    l.l_linenum = linenum;
    l.l_target = ecpyalloc(fields[LF_TARGET]);
    l.l_linkname = ecpyalloc(fields[LF_LINKNAME]);

    // Add to links array, expanding if needed
    links = growalloc(links, sizeof *links, nlinks, &nlinks_alloc);
    links[nlinks++] = l;
}
```