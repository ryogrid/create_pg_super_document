# getopt_long

## Location
[src/port/getopt_long.c:60-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/getopt_long.c#L60-L241)

## Overview
A GNU-style command line option parser that handles both short options (like -h) and long options (like --help) in PostgreSQL applications.

## Definition

```c
int
getopt_long(int argc, char *const argv[],
			const char *optstring,
			const struct option *longopts, int *longindex)
```
## Detailed Description
 is PostgreSQL's implementation of the GNU getopt_long function, used for parsing command-line arguments with support for both traditional short options and GNU-style long options. This implementation is provided in  as a compatibility layer for systems that don't have the GNU getopt_long function.

The function processes command line arguments sequentially, handling:
- Short options (single character preceded by a single dash, e.g., -h)
- Long options (multi-character preceded by double dash, e.g., --help)  
- Option arguments (both required and optional)
- Special option terminator "--" (stops option processing)
- Automatic reordering of non-option arguments to the end of argv

Key behavioral features:
- Reorders argv so all non-options appear at the end when parsing is complete
- Can be restarted on new argv arrays by resetting optind to 1
- Uses static variables to maintain state between calls
- Supports both flag-setting long options and value-returning long options
- Handles error reporting through opterr global variable

## Parameters / Member Variables
- `argc`: Number of command line arguments (including program name)
- `argv[]`: Array of command line argument strings
- `*optstring`: String specifying valid short options; ':' after option means it requires an argument
- `*longopts`: Array of struct option defining valid long options (terminated by entry with NULL name)
- `*longindex`: Output parameter - receives index of matched long option in longopts array (can be NULL)
## Dependencies  
- Functions called/Symbols referenced:
  -  (string parsing)
  -  (string length)
  -  (string comparison) 
  -  (character search)
  -  (error output)
  -  (empty string constant "")
  -  (bad character return value '?')  
  -  (bad argument return value ':')
  -  (long option definition structure)
  - Global variables: , , , 

- Called from (representative examples):
  -  functions in PostgreSQL utilities (initdb, pg_dump, psql, etc.)
  -  (pg_upgrade)
  -  (psql)
  -  (pg_test_fsync, pg_test_timing)

## Notes and Other Information
- This is a compatibility implementation used when the system doesn't provide GNU getopt_long
- The implementation reorders argv during parsing, moving non-options to the end
- Static variables maintain parsing state between calls: place, nonopt_start, force_nonopt
- Return values: option character for matched options, -1 when all options processed, '?' for unknown options, ':' for missing required arguments
- Unlike some getopt implementations, this does not use optreset for reinitialization
- Error messages are printed to stderr when opterr is non-zero
- Used extensively throughout PostgreSQL command-line utilities for consistent option parsing behavior

## Simplified Source

```c
int
getopt_long(int argc, char *const argv[],
            const char *optstring,
            const struct option *longopts, int *longindex)
{
    static char *place = EMSG;    // Current position in current argument
    static int nonopt_start = -1;  // Start of non-option arguments
    static bool force_nonopt = false; // Treat remaining args as non-options
    char *oli;

    if (!*place)
    {
        char **args = (char **) argv;

retry:
        // Return -1 if no more arguments or only non-options remain
        if (optind >= argc || optind == nonopt_start)
        {
            place = EMSG;
            nonopt_start = -1;
            force_nonopt = false;
            return -1;
        }

        place = argv[optind];

        // Handle non-options: move to end and retry
        if (force_nonopt || place[0] != '-' || place[1] == '\0')
        {
            // Shift non-option to end of argv
            for (int i = optind; i < argc - 1; i++)
                args[i] = args[i + 1];
            args[argc - 1] = place;

            // Track non-option start position
            if (nonopt_start == -1)
                nonopt_start = argc - 1;
            else
                nonopt_start--;

            goto retry;
        }

        place++;

        // Handle "--" (end of options marker)
        if (place[0] == '-' && place[1] == '\0')
        {
            ++optind;
            force_nonopt = true;
            goto retry;
        }

        // Handle long options (--option)
        if (place[0] == '-' && place[1])
        {
            place++;
            size_t namelen = strcspn(place, "=");

            // Search for matching long option
            for (int i = 0; longopts[i].name != NULL; i++)
            {
                if (strlen(longopts[i].name) == namelen &&
                    strncmp(place, longopts[i].name, namelen) == 0)
                {
                    int has_arg = longopts[i].has_arg;

                    // Handle option arguments
                    if (has_arg != no_argument)
                    {
                        if (place[namelen] == '=')
                            optarg = place + namelen + 1;
                        else if (optind < argc - 1 && has_arg == required_argument)
                        {
                            optind++;
                            optarg = argv[optind];
                        }
                        else
                        {
                            // Missing required argument
                            if (optstring[0] == ':')
                                return BADARG;
                            if (opterr && has_arg == required_argument)
                                fprintf(stderr, "%s: option requires an argument -- %s\n",
                                       argv[0], place);
                            place = EMSG;
                            optind++;
                            if (has_arg == required_argument)
                                return BADCH;
                            optarg = NULL;
                        }
                    }
                    else
                        optarg = NULL;

                    optind++;
                    if (longindex)
                        *longindex = i;
                    place = EMSG;

                    // Return value or set flag
                    if (longopts[i].flag == NULL)
                        return longopts[i].val;
                    else
                    {
                        *longopts[i].flag = longopts[i].val;
                        return 0;
                    }
                }
            }

            // Unknown long option
            if (opterr && optstring[0] != ':')
                fprintf(stderr, "%s: illegal option -- %s\n", argv[0], place);
            place = EMSG;
            optind++;
            return BADCH;
        }
    }

    // Handle short options (-o)
    optopt = (int) *place++;
    oli = strchr(optstring, optopt);

    if (!oli)
    {
        // Unknown short option
        if (!*place)
            ++optind;
        if (opterr && *optstring != ':')
            fprintf(stderr, "%s: illegal option -- %c\n", argv[0], optopt);
        return BADCH;
    }

    if (oli[1] != ':')
    {
        // Option doesn't need argument
        optarg = NULL;
        if (!*place)
            ++optind;
    }
    else
    {
        // Option needs argument
        if (*place)
            optarg = place;  // Argument attached (-oarg)
        else if (argc <= ++optind)
        {
            // Missing argument
            place = EMSG;
            if (*optstring == ':')
                return BADARG;
            if (opterr)
                fprintf(stderr, "%s: option requires an argument -- %c\n",
                       argv[0], optopt);
            return BADCH;
        }
        else
            optarg = argv[optind];  // Separate argument (-o arg)

        place = EMSG;
        ++optind;
    }

    return optopt;
}
```