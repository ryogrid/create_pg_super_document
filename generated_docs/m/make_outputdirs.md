# make_outputdirs

## Location
[src/bin/pg_upgrade/pg_upgrade.c:249-333](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.c#L249-L333)

## Overview
Creates and assigns proper permissions to the set of output directories used to store data generated internally by pg_upgrade, filling in log_opts structure with directory paths.

## Definition

```c
struct timeval time;
```
## Detailed Description
The make_outputdirs function is responsible for creating a structured directory hierarchy for pg_upgrade output files and logs. It creates a timestamped directory structure under the PostgreSQL data directory to organize upgrade-related files. The function:

- Creates a base output directory with timestamp-based subdirectories
- Establishes separate directories for dumps and logs  
- Sets up file handles for internal logging
- Initializes all log files with upgrade run timestamps
- Uses millisecond precision timestamps to avoid conflicts between concurrent runs

The directory structure created follows the pattern:  with subdirectories for dumps and logs.

## Parameters / Member Variables
- : Path to the PostgreSQL data directory where output directories will be created

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc0](../p/pg_malloc0.md) (memory allocation)
  - [gettimeofday](../g/gettimeofday.md) (high precision timestamp)
  - strftime (timestamp formatting)
  - mkdir (directory creation)
  - fopen_priv (secure file opening)
  - BASE_OUTPUTDIR (output directory name constant)
  - DUMP_OUTPUTDIR (dump subdirectory name)
  - LOG_OUTPUTDIR (log subdirectory name)
  - INTERNAL_LOG_FILE (internal log filename)
- Called from:
  - [main](main.md) (from pg_upgrade.c:124)

## Notes and Other Information
- Uses millisecond precision timestamps to prevent directory name collisions
- Handles the case where root directory already exists (for multiple upgrade attempts)
- Creates directories with pg_dir_create_mode permissions
- Initializes all output log files with upgrade start timestamps
- Critical for organizing pg_upgrade output and maintaining upgrade history
- Part of the pg_upgrade utility's initialization sequence

## Simplified Source

```c
static void
make_outputdirs(char *pgdata)
{
    FILE *fp;
    char **filename;
    time_t run_time = time(NULL);
    char filename_path[MAXPGPATH];
    char timebuf[128];
    struct timeval time;
    int len;

    // Create base directory path: pgdata/pg_upgrade_output
    log_opts.rootdir = (char *) pg_malloc0(MAXPGPATH);
    len = snprintf(log_opts.rootdir, MAXPGPATH, "%s/%s", pgdata, BASE_OUTPUTDIR);
    if (len >= MAXPGPATH)
        pg_fatal("directory path for new cluster is too long");

    // Create timestamped subdirectory with millisecond precision
    gettimeofday(&time, NULL);
    time_t tt = (time_t) time.tv_sec;
    strftime(timebuf, sizeof(timebuf), "%Y%m%dT%H%M%S", localtime(&tt));
    snprintf(timebuf + strlen(timebuf), sizeof(timebuf) - strlen(timebuf),
             ".%03d", (int) (time.tv_usec / 1000));

    // Create timestamped base directory
    log_opts.basedir = (char *) pg_malloc0(MAXPGPATH);
    len = snprintf(log_opts.basedir, MAXPGPATH, "%s/%s", log_opts.rootdir, timebuf);
    if (len >= MAXPGPATH)
        pg_fatal("directory path for new cluster is too long");

    // Create dump and log subdirectories
    log_opts.dumpdir = (char *) pg_malloc0(MAXPGPATH);
    len = snprintf(log_opts.dumpdir, MAXPGPATH, "%s/%s/%s",
                   log_opts.rootdir, timebuf, DUMP_OUTPUTDIR);
    if (len >= MAXPGPATH)
        pg_fatal("directory path for new cluster is too long");

    log_opts.logdir = (char *) pg_malloc0(MAXPGPATH);
    len = snprintf(log_opts.logdir, MAXPGPATH, "%s/%s/%s",
                   log_opts.rootdir, timebuf, LOG_OUTPUTDIR);
    if (len >= MAXPGPATH)
        pg_fatal("directory path for new cluster is too long");

    // Create all directories (ignore EEXIST for root directory)
    if (mkdir(log_opts.rootdir, pg_dir_create_mode) < 0 && errno != EEXIST)
        pg_fatal("could not create directory \"%s\": %m", log_opts.rootdir);
    if (mkdir(log_opts.basedir, pg_dir_create_mode) < 0)
        pg_fatal("could not create directory \"%s\": %m", log_opts.basedir);
    if (mkdir(log_opts.dumpdir, pg_dir_create_mode) < 0)
        pg_fatal("could not create directory \"%s\": %m", log_opts.dumpdir);
    if (mkdir(log_opts.logdir, pg_dir_create_mode) < 0)
        pg_fatal("could not create directory \"%s\": %m", log_opts.logdir);

    // Open internal log file
    len = snprintf(filename_path, sizeof(filename_path), "%s/%s",
                   log_opts.logdir, INTERNAL_LOG_FILE);
    if (len >= sizeof(filename_path))
        pg_fatal("directory path for new cluster is too long");
    if ((log_opts.internal = fopen_priv(filename_path, "a")) == NULL)
        pg_fatal("could not open log file \"%s\": %m", filename_path);

    // Initialize all output log files with upgrade start timestamp
    for (filename = output_files; *filename != NULL; filename++)
    {
        len = snprintf(filename_path, sizeof(filename_path), "%s/%s",
                       log_opts.logdir, *filename);
        if (len >= sizeof(filename_path))
            pg_fatal("directory path for new cluster is too long");
        if ((fp = fopen_priv(filename_path, "a")) == NULL)
            pg_fatal("could not write to log file \"%s\": %m", filename_path);

        fprintf(fp,
                "-----------------------------------------------------------------\n"
                "  pg_upgrade run on %s"
                "-----------------------------------------------------------------\n\n",
                ctime(&run_time));
        fclose(fp);
    }
}
```