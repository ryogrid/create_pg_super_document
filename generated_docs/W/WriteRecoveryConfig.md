# WriteRecoveryConfig

## Location
[src/fe_utils/recovery_gen.c:124-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/recovery_gen.c#L124-L162)

## Overview
WriteRecoveryConfig writes recovery configuration content to the appropriate configuration file and creates necessary signal files for PostgreSQL standby server setup.

## Definition

```c
void
WriteRecoveryConfig(PGconn *pgconn, const char *target_dir, PQExpBuffer contents)
```
## Detailed Description
This function writes the recovery configuration to the filesystem, handling version-specific differences in PostgreSQL's recovery configuration mechanism. For PostgreSQL versions prior to 12, it writes to recovery.conf. For version 12 and later, it appends to postgresql.auto.conf and creates a standby.signal file to trigger standby mode.

The function determines the appropriate configuration method based on the server version, writes the provided configuration content to the correct file, and creates the standby.signal file when necessary. This ensures proper standby server initialization across different PostgreSQL versions.

## Parameters / Member Variables
- `*pgconn`: Database connection used to determine server version
- `*target_dir`: Directory path where configuration files should be written
- `contents`: PQExpBuffer containing the recovery configuration content to write
## Dependencies
- Functions called/Symbols referenced:
  - [PQserverVersion](../P/PQserverVersion.md)
  - MINIMUM_VERSION_FOR_RECOVERY_GUC
  - snprintf
  - fopen
  - fwrite
  - fclose
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [setup_recovery](../s/setup_recovery.md) (pg_createsubscriber.c:1237)
  - [main](../m/main.md) (pg_rewind.c:453, 530)

## Notes and Other Information
- Handles PostgreSQL version differences: uses recovery.conf for pre-12 versions, postgresql.auto.conf + standby.signal for 12+
- For older versions, opens recovery.conf in write mode ('w'), completely replacing its contents
- For newer versions, opens postgresql.auto.conf in append mode ('a'), preserving existing configuration
- Creates an empty standby.signal file for PostgreSQL 12+ to trigger standby mode
- Calls pg_fatal() on file operation failures
- The function assumes the target directory already exists and is writable

## Simplified Source

```c
void WriteRecoveryConfig(PGconn *pgconn, const char *target_dir, PQExpBuffer contents) {
    char filename[MAXPGPATH];

    // Determine configuration method based on PostgreSQL version
    bool use_recovery_conf = PQserverVersion(pgconn) < MINIMUM_VERSION_FOR_RECOVERY_GUC;

    // Build filename for configuration file
    snprintf(filename, MAXPGPATH, "%s/%s", target_dir,
             use_recovery_conf ? "recovery.conf" : "postgresql.auto.conf");

    // Open configuration file (write mode for recovery.conf, append for postgresql.auto.conf)
    FILE *cf = fopen(filename, use_recovery_conf ? "w" : "a");
    if (!cf)
        pg_fatal("could not open file \"%s\": %m", filename);

    // Write configuration content
    if (fwrite(contents->data, contents->len, 1, cf) != 1)
        pg_fatal("could not write to file \"%s\": %m", filename);

    fclose(cf);

    // For PostgreSQL 12+, create standby.signal file to trigger standby mode
    if (!use_recovery_conf) {
        snprintf(filename, MAXPGPATH, "%s/standby.signal", target_dir);
        cf = fopen(filename, "w");
        if (!cf)
            pg_fatal("could not create file \"%s\": %m", filename);
        fclose(cf);
    }
}
```