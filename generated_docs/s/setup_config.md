setup_config

## Overview
Creates and configures all PostgreSQL configuration files (postgresql.conf, postgresql.auto.conf, pg_hba.conf, pg_ident.conf) during database cluster initialization.

## Definition
static void setup_config(void)

## Detailed Description
This comprehensive function is responsible for setting up all the essential configuration files required for a PostgreSQL database cluster during initdb. It reads template configuration files and customizes them based on the initialization parameters, system capabilities, and user-specified options.

The function performs several key operations:
1. Sets up postgresql.conf with appropriate values for max_connections, shared_buffers, locale settings, timezone, WAL settings, and various other GUC parameters
2. Creates postgresql.auto.conf as an empty file with warning comments
3. Configures pg_hba.conf with authentication methods and IPv6 support detection
4. Sets up pg_ident.conf for user name mapping

The function automatically determines appropriate values for many settings, such as formatting shared_buffers with optimal units (MB/kB), setting datestyle based on locale, and configuring WAL sizes using pretty_wal_size(). It also handles platform-specific settings and applies any user-specified GUC overrides via command-line options.

## Parameters / Member Variables
This function takes no parameters but operates on numerous global variables including:
- n_connections, n_buffers: Connection and buffer settings
- Various locale variables: lc_messages, lc_monetary, lc_numeric, lc_time
- Authentication method variables: authmethodlocal, authmethodhost
- Configuration file paths: conf_file, hba_file, ident_file
- extra_guc_names, extra_guc_values: User-specified overrides

## Dependencies
- Functions called/Symbols referenced:
  - [readfile](../r/readfile.md), writefile
  - [replace_guc_value](../r/replace_guc_value.md), replace_token
  - [pretty_wal_size](../p/pretty_wal_size.md)
  - [locale_date_order](../l/locale_date_order.md)
  - pg_malloc_array, pg_strdup
  - [check_ok](../c/check_ok.md)
- Called from (representative examples):
  - [initialize_data_directory](../i/initialize_data_directory.md) (around line 3079)

## Notes and Other Information
- This is a static function within initdb.c, used specifically during database cluster initialization
- The function handles platform-specific configurations (Windows, IPv6 support detection)
- Automatically formats memory and WAL size values with appropriate units for readability
- Sets appropriate file permissions using pg_file_create_mode
- Includes IPv6 capability detection to avoid runtime warnings on systems without IPv6 support
- Handles special cases like setting password_encryption based on authentication methods
- Supports group access permissions when pg_dir_create_mode is set to PG_DIR_MODE_GROUP
- Template files are read and token replacement is used to customize configuration values

## Simplified Source

```c
static void setup_config(void) {
    char **conflines;
    char repltok[MAXPGPATH];
    char path[MAXPGPATH];

    fputs(_("creating configuration files ... "), stdout);
    fflush(stdout);

    // Configure postgresql.conf
    conflines = readfile(conf_file);

    // Set max_connections
    snprintf(repltok, sizeof(repltok), "%d", n_connections);
    conflines = replace_guc_value(conflines, "max_connections", repltok, false);

    // Set shared_buffers with appropriate units
    if ((n_buffers * (BLCKSZ / 1024)) % 1024 == 0)
        snprintf(repltok, sizeof(repltok), "%dMB", (n_buffers * (BLCKSZ / 1024)) / 1024);
    else
        snprintf(repltok, sizeof(repltok), "%dkB", n_buffers * (BLCKSZ / 1024));
    conflines = replace_guc_value(conflines, "shared_buffers", repltok, false);

    // Set locale settings
    conflines = replace_guc_value(conflines, "lc_messages", lc_messages, false);
    conflines = replace_guc_value(conflines, "lc_monetary", lc_monetary, false);
    conflines = replace_guc_value(conflines, "lc_numeric", lc_numeric, false);
    conflines = replace_guc_value(conflines, "lc_time", lc_time, false);

    // Set datestyle based on locale date order
    switch (locale_date_order(lc_time)) {
        case DATEORDER_YMD: strcpy(repltok, "iso, ymd"); break;
        case DATEORDER_DMY: strcpy(repltok, "iso, dmy"); break;
        case DATEORDER_MDY:
        default: strcpy(repltok, "iso, mdy"); break;
    }
    conflines = replace_guc_value(conflines, "datestyle", repltok, false);

    // Set WAL sizes using pretty formatting
    conflines = replace_guc_value(conflines, "min_wal_size",
                                  pretty_wal_size(DEFAULT_MIN_WAL_SEGS), false);
    conflines = replace_guc_value(conflines, "max_wal_size",
                                  pretty_wal_size(DEFAULT_MAX_WAL_SEGS), false);

    // Apply user-specified GUC overrides
    for (gnames = extra_guc_names, gvalues = extra_guc_values;
         gnames != NULL; gnames = gnames->next, gvalues = gvalues->next) {
        conflines = replace_guc_value(conflines, gnames->str, gvalues->str, false);
    }

    // Write postgresql.conf
    snprintf(path, sizeof(path), "%s/postgresql.conf", pg_data);
    writefile(path, conflines);
    chmod(path, pg_file_create_mode);

    // Create postgresql.auto.conf with warning comments
    // Configure pg_hba.conf with authentication methods and IPv6 detection
    // Set up pg_ident.conf

    check_ok();
}
```