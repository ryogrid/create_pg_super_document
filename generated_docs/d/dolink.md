# dolink

## Location
[src/timezone/zic.c:1004-1105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1004-L1105)

## Overview
Creates a link (hard link, symbolic link, or file copy) from a target file to a linkname, implementing a fallback strategy when preferred linking methods fail.

## Definition

```c
struct stat st;
```
## Detailed Description
The  function implements a comprehensive linking strategy for timezone files. It attempts to create links in the following priority order:
1. Hard link (via )
2. Symbolic link (if hard linking fails and HAVE_SYMLINK is defined)
3. File copy (as final fallback)

The function handles special cases like removing existing links, creating necessary directories, and managing symbolic link preservation. It also supports a "remove only" mode when target is "-".

The function carefully manages error conditions and provides appropriate warnings when fallback methods are used. It ensures that directories are not used as link targets and handles the case where the linkname should remain a symbolic link if it was one previously.

## Parameters / Member Variables
- : The path to the source file to be linked, or "-" for remove-only operation
- : The path where the link should be created
- : Boolean flag indicating whether to preserve existing symbolic link behavior

## Dependencies
- Functions called/Symbols referenced:
  - [itsdir](../i/itsdir.md) (checks if target is a directory)
  - [itssymlink](../i/itssymlink.md) (checks if linkname is a symbolic link)
  - [hardlinkerr](../h/hardlinkerr.md) (attempts to create hard link)
  - [relname](../r/relname.md) (generates relative path for symbolic links)
  - [mkdirs](../m/mkdirs.md) (creates necessary parent directories)
  - symlink (POSIX system call for symbolic links)
  - [warning](../w/warning.md) (outputs warning messages)
  - [close_file](../c/close_file.md) (closes files safely)
  - Standard C library functions: strcmp, remove, strerror, fopen, getc, putc, free
- Called from:
  - [main](../m/main.md) (three times, at lines 836, 846, and 851 in src/timezone/zic.c)

## Notes and Other Information
- This is a static function local to src/timezone/zic.c, part of PostgreSQL's timezone handling code
- Implements a robust fallback strategy: hard link → symbolic link → file copy
- Handles security considerations for running with elevated privileges
- Provides detailed error messages and warnings for troubleshooting
- Supports both absolute and relative symbolic links
- The function exits the program on critical errors rather than returning error codes
- Conditional compilation with HAVE_SYMLINK macro for systems without symbolic link support

## Simplified Source

```c
static void
dolink(char const *target, char const *linkname, bool staysymlink)
{
    bool remove_only = strcmp(target, "-") == 0;
    bool linkdirs_made = false;
    int link_errno;

    // Safety check: don't link directories
    if (!remove_only && itsdir(target)) {
        fprintf(stderr, _("%s: linking target %s/%s failed: %s\n"),
                progname, directory, target, strerror(EPERM));
        exit(EXIT_FAILURE);
    }

    // Check if we should preserve symlink behavior
    if (staysymlink)
        staysymlink = itssymlink(linkname);

    // Remove existing link
    if (remove(linkname) == 0)
        linkdirs_made = true;
    else if (errno != ENOENT) {
        fprintf(stderr, _("%s: Can't remove %s/%s: %s\n"),
                progname, directory, linkname, strerror(errno));
        exit(EXIT_FAILURE);
    }

    if (remove_only)
        return;

    // Try hard link first
    link_errno = staysymlink ? ENOTSUP : hardlinkerr(target, linkname);
    if (link_errno == ENOENT && !linkdirs_made) {
        mkdirs(linkname, true);
        linkdirs_made = true;
        link_errno = hardlinkerr(target, linkname);
    }

    // Fallback to symlink or copy if hard link fails
    if (link_errno != 0) {
#ifdef HAVE_SYMLINK
        // Try symbolic link
        bool absolute = *target == '/';
        char *linkalloc = absolute ? NULL : relname(target, linkname);
        char const *contents = absolute ? target : linkalloc;
        int symlink_errno = symlink(contents, linkname) == 0 ? 0 : errno;

        if (!linkdirs_made && symlink_errno == ENOENT) {
            mkdirs(linkname, true);
            symlink_errno = symlink(contents, linkname) == 0 ? 0 : errno;
        }

        free(linkalloc);
        if (symlink_errno == 0) {
            if (link_errno != ENOTSUP)
                warning(_("symbolic link used because hard link failed: %s"),
                        strerror(link_errno));
        } else
#endif
        {
            // Final fallback: file copy
            FILE *fp = fopen(target, "rb");
            if (!fp) {
                fprintf(stderr, _("%s: Can't read %s/%s: %s\n"),
                        progname, directory, target, strerror(errno));
                exit(EXIT_FAILURE);
            }

            FILE *tp = fopen(linkname, "wb");
            if (!tp) {
                fprintf(stderr, _("%s: Can't create %s/%s: %s\n"),
                        progname, directory, linkname, strerror(errno));
                exit(EXIT_FAILURE);
            }

            // Copy file contents
            int c;
            while ((c = getc(fp)) != EOF)
                putc(c, tp);

            close_file(fp, directory, target);
            close_file(tp, directory, linkname);

            if (link_errno != ENOTSUP)
                warning(_("copy used because hard link failed: %s"),
                        strerror(link_errno));
        }
    }
}
```