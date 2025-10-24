# get_control_data

## Location
[src/bin/pg_upgrade/controldata.c:36-653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/controldata.c#L36-L653)

## Overview
Extracts pg_control information from PostgreSQL clusters in a version-independent manner by invoking pg_controldata or pg_resetwal and parsing their output.

## Definition

```c
struct the
	 * WAL file name from the xlogid and segno.
	 */
	if (GET_MAJOR_VERSION(cluster->major_version) <= 902)
	{
		if (got_tli && got_log_id && got_log_seg)
		{
			snprintf(cluster->controldata.nextxlogfile, 25, "%08X%08X%08X",
					 tli, logid, segno);
			got_nextxlogfile = true;
		}
	}

	/* verify that we got all the mandatory pg_control data */
	if (!got_xid || !got_oid ||
		!got_multi || !got_oldestxid ||
		(!got_oldestmulti &&
		 cluster->controldata.cat_ver >= MULTIXACT_FORMATCHANGE_CAT_VER) ||
		!got_mxoff || (!live_check && !got_nextxlogfile) ||
		!got_float8_pass_by_value || !got_align || !got_blocksz ||
		!got_largesz || !got_walsz || !got_walseg || !got_ident ||
		!got_index || !got_toast ||
		(!got_large_object &&
		 cluster->controldata.ctrl_ver >= LARGE_OBJECT_SIZE_PG_CONTROL_VER) ||
		!got_date_is_int || !got_data_checksum_version)
	{
		if (cluster == &old_cluster)
			pg_log(PG_REPORT,
				   "The source cluster lacks some required control information:");
		else
			pg_log(PG_REPORT,
				   "The target cluster lacks some required control information:");

		if (!got_xid)
			pg_log(PG_REPORT, "  checkpoint next XID");

		if (!got_oid)
			pg_log(PG_REPORT, "  latest checkpoint next OID");

		if (!got_multi)
			pg_log(PG_REPORT, "  latest checkpoint next MultiXactId");

		if (!got_oldestmulti &&
			cluster->controldata.cat_ver >= MULTIXACT_FORMATCHANGE_CAT_VER)
			pg_log(PG_REPORT, "  latest checkpoint oldest MultiXactId");

		if (!got_oldestxid)
			pg_log(PG_REPORT, "  latest checkpoint oldestXID");

		if (!got_mxoff)
			pg_log(PG_REPORT, "  latest checkpoint next MultiXactOffset");

		if (!live_check && !got_nextxlogfile)
			pg_log(PG_REPORT, "  first WAL segment after reset");

		if (!got_float8_pass_by_value)
			pg_log(PG_REPORT, "  float8 argument passing method");

		if (!got_align)
			pg_log(PG_REPORT, "  maximum alignment");

		if (!got_blocksz)
			pg_log(PG_REPORT, "  block size");

		if (!got_largesz)
			pg_log(PG_REPORT, "  large relation segment size");

		if (!got_walsz)
			pg_log(PG_REPORT, "  WAL block size");

		if (!got_walseg)
			pg_log(PG_REPORT, "  WAL segment size");

		if (!got_ident)
			pg_log(PG_REPORT, "  maximum identifier length");

		if (!got_index)
			pg_log(PG_REPORT, "  maximum number of indexed columns");

		if (!got_toast)
			pg_log(PG_REPORT, "  maximum TOAST chunk size");

		if (!got_large_object &&
			cluster->controldata.ctrl_ver >= LARGE_OBJECT_SIZE_PG_CONTROL_VER)
			pg_log(PG_REPORT, "  large-object chunk size");

		if (!got_date_is_int)
			pg_log(PG_REPORT, "  dates/times are integers?");

		/* value added in Postgres 9.3 */
		if (!got_data_checksum_version)
			pg_log(PG_REPORT, "  data checksum version");

		pg_fatal("Cannot continue without required control information, terminating");
	}
}


/*
 * check_control_data()
 *
 * check to make sure the control data settings are compatible
 */
void
check_control_data(ControlData *oldctrl,
				   ControlData *newctrl)
{
	if (oldctrl->align == 0 || oldctrl->align != newctrl->align)
		pg_fatal("old and new pg_controldata alignments are invalid or do not match.\n"
				 "Likely one cluster is a 32-bit install, the other 64-bit");
```
## Detailed Description
The  function is a core component of pg_upgrade that extracts critical control data from PostgreSQL clusters. It handles version differences by using different utilities:

- For live checks or when examining the new cluster: Uses  to read control information from a running or shutdown server
- For offline checks on the old cluster: Uses  (or  for versions ≤ 9.6) to simulate what the control data would be after a reset

The function sets up a controlled environment by manipulating locale variables to ensure English output, then parses the utility output line-by-line to extract essential parameters like transaction IDs, checkpoint information, database configuration parameters, and WAL settings.

Key validation includes:
- Verifying cluster shutdown state (must be cleanly shut down, not in recovery)
- Ensuring all mandatory control data fields are present
- Handling version-specific output format differences
- Constructing WAL filenames for older versions (≤ 9.2) from separate log ID and segment components

## Parameters / Member Variables
- `segno)`: ClusterInfo structure to populate with extracted control data
- `true`: Boolean indicating whether this is a live server check (true) or offline analysis (false)

## Dependencies
- Functions called/Symbols referenced:
  - popen/pclose (system process execution)
  - [pg_strip_crlf](../p/pg_strip_crlf.md) (string processing)
  - [str2uint](../s/str2uint.md) (string to integer conversion)
  - setenv/unsetenv (environment manipulation)
  - [pg_log](../p/pg_log.md) (logging)
  - [pg_fatal](../p/pg_fatal.md) (error handling)
  - [strlcpy](../s/strlcpy.md) (safe string copying)
  - [pg_free](../p/pg_free.md) (memory management)
- Called from (representative examples):
  - [check_cluster_compatibility](../c/check_cluster_compatibility.md) (src/bin/pg_upgrade/check.c:842-843)

## Notes and Other Information
- Temporarily modifies locale environment variables to force English output for reliable parsing
- Handles multiple PostgreSQL version differences in control data format and utility names
- Critical for upgrade compatibility checking as it provides the foundation data for comparing old and new clusters
- The function includes extensive error checking and detailed reporting of missing control information
- WAL filename construction differs between PostgreSQL versions, requiring version-specific logic

## Simplified Source

```c
void get_control_data(ClusterInfo *cluster, bool live_check) {
    char cmd[MAXPGPATH];
    char bufin[MAX_STRING];
    FILE *output;
    char *p;

    // Tracking variables for parsed control data
    bool got_xid = false, got_oid = false, got_multi = false;
    bool got_oldestxid = false, got_nextxlogfile = false;
    bool got_float8_pass_by_value = false, got_align = false;
    bool got_blocksz = false, got_walsz = false, got_walseg = false;
    // ... additional tracking variables for other fields

    // Set environment to English for reliable parsing
    char *saved_locale_vars[8];  // Save current locale settings
    // Save current locale environment variables
    // Set LC_MESSAGES=C and unset other locale variables

    // Check cluster shutdown state (for non-live checks)
    if (!live_check || cluster == &new_cluster) {
        snprintf(cmd, sizeof(cmd), "\"%s/pg_controldata\" \"%s\"",
                cluster->bindir, cluster->pgdata);

        output = popen(cmd, "r");
        if (!output)
            pg_fatal("could not get control data using %s", cmd);

        // Parse output for database cluster state
        while (fgets(bufin, sizeof(bufin), output)) {
            if (strstr(bufin, "Database cluster state:")) {
                // Extract and validate cluster state
                // Must be "shut down", not "shut down in recovery"
                // ... state validation logic
            }
        }
        pclose(output);
    }

    // Main control data extraction
    char *resetwal_bin = (GET_MAJOR_VERSION(cluster->bin_version) <= 906)
                        ? "pg_resetxlog\" -n" : "pg_resetwal\" -n";

    snprintf(cmd, sizeof(cmd), "\"%s/%s \"%s\"",
            cluster->bindir,
            live_check ? "pg_controldata\"" : resetwal_bin,
            cluster->pgdata);

    output = popen(cmd, "r");
    if (!output)
        pg_fatal("could not get control data using %s", cmd);

    // Parse output line by line
    while (fgets(bufin, sizeof(bufin), output)) {
        pg_strip_crlf(bufin);

        // Extract pg_control version
        if ((p = strstr(bufin, "pg_control version number:")) != NULL) {
            p = strchr(p, ':') + 1;
            cluster->controldata.ctrl_ver = str2uint(p);
        }
        // Extract catalog version
        else if ((p = strstr(bufin, "Catalog version number:")) != NULL) {
            p = strchr(p, ':') + 1;
            cluster->controldata.cat_ver = str2uint(p);
        }
        // Extract NextXID with version-specific delimiter handling
        else if ((p = strstr(bufin, "Latest checkpoint's NextXID:")) != NULL) {
            p = strchr(p, ':') + 1;
            cluster->controldata.chkpnt_nxtepoch = str2uint(p);

            // Handle version differences in delimiter ('/' vs ':')
            p = (strchr(p, '/') != NULL) ? strchr(p, '/') : strchr(p, ':');
            if (p) {
                p++;
                cluster->controldata.chkpnt_nxtxid = str2uint(p);
                got_xid = true;
            }
        }
        // Extract other checkpoint values (OID, MultiXact, etc.)
        else if ((p = strstr(bufin, "Latest checkpoint's NextOID:")) != NULL) {
            p = strchr(p, ':') + 1;
            cluster->controldata.chkpnt_nxtoid = str2uint(p);
            got_oid = true;
        }
        // ... similar parsing for other control data fields

        // Extract configuration parameters
        else if ((p = strstr(bufin, "Float8 argument passing:")) != NULL) {
            p = strchr(p, ':') + 1;
            cluster->controldata.float8_pass_by_value = (strstr(p, "by value") != NULL);
            got_float8_pass_by_value = true;
        }
        else if ((p = strstr(bufin, "Database block size:")) != NULL) {
            p = strchr(p, ':') + 1;
            cluster->controldata.blocksz = str2uint(p);
            got_blocksz = true;
        }
        // ... additional configuration parameter parsing
    }

    pclose(output);

    // Restore environment variables
    // ... restore saved locale settings

    // Handle version differences for WAL filename construction
    if (GET_MAJOR_VERSION(cluster->major_version) <= 902) {
        // Older versions: construct WAL filename from separate components
        if (got_tli && got_log_id && got_log_seg) {
            snprintf(cluster->controldata.nextxlogfile, 25, "%08X%08X%08X",
                    tli, logid, segno);
            got_nextxlogfile = true;
        }
    }

    // Verify all mandatory fields were found
    if (!got_xid || !got_oid || !got_multi || !got_oldestxid ||
        !got_float8_pass_by_value || !got_align || !got_blocksz ||
        !got_walsz || !got_walseg || (!live_check && !got_nextxlogfile)) {

        // Report missing fields in detail
        pg_log(PG_REPORT, "Required control information missing:");
        if (!got_xid) pg_log(PG_REPORT, "  checkpoint next XID");
        if (!got_oid) pg_log(PG_REPORT, "  latest checkpoint next OID");
        // ... report all missing fields

        pg_fatal("Cannot continue without required control information");
    }
}
```