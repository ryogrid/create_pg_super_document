# scan_available_timezones

## Location
[src/bin/initdb/findtimezone.c:657-1564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/findtimezone.c#L657-L1564)

## Overview
Recursively scans the timezone database directory looking for the best match to the system timezone behavior, comparing timezone files against system timezone characteristics.

## Definition

```c
struct tztry *tt,
						 int *bestscore, char *bestzonename)
{
	int			tzdir_orig_len = strlen(tzdir);
	char	  **names;
	char	  **namep;

	names = pgfnames(tzdir);
	if (!names)
		return;

	for (namep = names; *namep; namep++)
	{
		char	   *name = *namep;
		struct stat statbuf;

		/* Ignore . and .., plus any other "hidden" files */
		if (name[0] == '.')
			continue;

		snprintf(tzdir + tzdir_orig_len, MAXPGPATH - tzdir_orig_len,
				 "/%s", name);

		if (stat(tzdir, &statbuf) != 0)
		{
#ifdef DEBUG_IDENTIFY_TIMEZONE
			fprintf(stderr, "could not stat \"%s\": %m\n",
					tzdir);
#endif
			tzdir[tzdir_orig_len] = '\0';
			continue;
		}

		if (S_ISDIR(statbuf.st_mode))
		{
			/* Recurse into subdirectory */
			scan_available_timezones(tzdir, tzdirsub, tt,
									 bestscore, bestzonename);
		}
		else
		{
			/* Load and test this file */
			int			score = score_timezone(tzdirsub, tt);

			if (score > *bestscore)
			{
				*bestscore = score;
				strlcpy(bestzonename, tzdirsub, TZ_STRLEN_MAX + 1);
			}
			else if (score == *bestscore)
			{
				/* Consider how to break a tie */
				int			namepref = (zone_name_pref(tzdirsub) -
										zone_name_pref(bestzonename));

				if (namepref > 0 ||
					(namepref == 0 &&
					 (strlen(tzdirsub) < strlen(bestzonename) ||
					  (strlen(tzdirsub) == strlen(bestzonename) &&
					   strcmp(tzdirsub, bestzonename) < 0))))
					strlcpy(bestzonename, tzdirsub, TZ_STRLEN_MAX + 1);
			}
		}

		/* Restore tzdir */
		tzdir[tzdir_orig_len] = '\0';
	}

	pgfnames_cleanup(names);
}
#else							/* WIN32 */

static const struct
{
	const char *stdname;		/* Windows name of standard timezone */
	const char *dstname;		/* Windows name of daylight timezone */
	const char *pgtzname;		/* Name of pgsql timezone to map to */
}			win32_tzmap[] =

{
	/*
	 * This list was built from the contents of the registry at
	 * HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Time
	 * Zones on Windows 7, Windows 10, and Windows Server 2019.  Some recent
	 * additions have been made by comparing to the CLDR project's
	 * windowsZones.xml file.
	 *
	 * The zones have been matched to IANA timezones based on CLDR's mapping
	 * for "territory 001".
	 */
	{
		/* (UTC+04:30) Kabul */
		"Afghanistan Standard Time", "Afghanistan Daylight Time",
		"Asia/Kabul"
	},
	{
		/* (UTC-09:00) Alaska */
		"Alaskan Standard Time", "Alaskan Daylight Time",
		"America/Anchorage"
	},
	{
		/* (UTC-10:00) Aleutian Islands */
		"Aleutian Standard Time", "Aleutian Daylight Time",
		"America/Adak"
	},
	{
		/* (UTC+07:00) Barnaul, Gorno-Altaysk */
		"Altai Standard Time", "Altai Daylight Time",
		"Asia/Barnaul"
	},
	{
		/* (UTC+03:00) Kuwait, Riyadh */
		"Arab Standard Time", "Arab Daylight Time",
		"Asia/Riyadh"
	},
	{
		/* (UTC+04:00) Abu Dhabi, Muscat */
		"Arabian Standard Time", "Arabian Daylight Time",
		"Asia/Dubai"
	},
	{
		/* (UTC+03:00) Baghdad */
		"Arabic Standard Time", "Arabic Daylight Time",
		"Asia/Baghdad"
	},
	{
		/* (UTC-03:00) City of Buenos Aires */
		"Argentina Standard Time", "Argentina Daylight Time",
		"America/Buenos_Aires"
	},
	{
		/* (UTC+04:00) Baku, Tbilisi, Yerevan */
		"Armenian Standard Time", "Armenian Daylight Time",
		"Asia/Yerevan"
	},
	{
		/* (UTC+04:00) Astrakhan, Ulyanovsk */
		"Astrakhan Standard Time", "Astrakhan Daylight Time",
		"Europe/Astrakhan"
	},
	{
		/* (UTC-04:00) Atlantic Time (Canada) */
		"Atlantic Standard Time", "Atlantic Daylight Time",
		"America/Halifax"
	},
	{
		/* (UTC+09:30) Darwin */
		"AUS Central Standard Time", "AUS Central Daylight Time",
		"Australia/Darwin"
	},
	{
		/* (UTC+08:45) Eucla */
		"Aus Central W. Standard Time", "Aus Central W. Daylight Time",
		"Australia/Eucla"
	},
	{
		/* (UTC+10:00) Canberra, Melbourne, Sydney */
		"AUS Eastern Standard Time", "AUS Eastern Daylight Time",
		"Australia/Sydney"
	},
	{
		/* (UTC+04:00) Baku */
		"Azerbaijan Standard Time", "Azerbaijan Daylight Time",
		"Asia/Baku"
	},
	{
		/* (UTC-01:00) Azores */
		"Azores Standard Time", "Azores Daylight Time",
		"Atlantic/Azores"
	},
	{
		/* (UTC-03:00) Salvador */
		"Bahia Standard Time", "Bahia Daylight Time",
		"America/Bahia"
	},
	{
		/* (UTC+06:00) Dhaka */
		"Bangladesh Standard Time", "Bangladesh Daylight Time",
		"Asia/Dhaka"
	},
	{
		/* (UTC+03:00) Minsk */
		"Belarus Standard Time", "Belarus Daylight Time",
		"Europe/Minsk"
	},
	{
		/* (UTC+11:00) Bougainville Island */
		"Bougainville Standard Time", "Bougainville Daylight Time",
		"Pacific/Bougainville"
	},
	{
		/* (UTC-01:00) Cabo Verde Is. */
		"Cabo Verde Standard Time", "Cabo Verde Daylight Time",
		"Atlantic/Cape_Verde"
	},
	{
		/* (UTC-06:00) Saskatchewan */
		"Canada Central Standard Time", "Canada Central Daylight Time",
		"America/Regina"
	},
	{
		/* (UTC-01:00) Cape Verde Is. */
		"Cape Verde Standard Time", "Cape Verde Daylight Time",
		"Atlantic/Cape_Verde"
	},
	{
		/* (UTC+04:00) Yerevan */
		"Caucasus Standard Time", "Caucasus Daylight Time",
		"Asia/Yerevan"
	},
	{
		/* (UTC+09:30) Adelaide */
		"Cen. Australia Standard Time", "Cen. Australia Daylight Time",
		"Australia/Adelaide"
	},
	{
		/* (UTC-06:00) Central America */
		"Central America Standard Time", "Central America Daylight Time",
		"America/Guatemala"
	},
	{
		/* (UTC+06:00) Astana */
		"Central Asia Standard Time", "Central Asia Daylight Time",
		"Asia/Almaty"
	},
	{
		/* (UTC-04:00) Cuiaba */
		"Central Brazilian Standard Time", "Central Brazilian Daylight Time",
		"America/Cuiaba"
	},
	{
		/* (UTC+01:00) Belgrade, Bratislava, Budapest, Ljubljana, Prague */
		"Central Europe Standard Time", "Central Europe Daylight Time",
		"Europe/Budapest"
	},
	{
		/* (UTC+01:00) Sarajevo, Skopje, Warsaw, Zagreb */
		"Central European Standard Time", "Central European Daylight Time",
		"Europe/Warsaw"
	},
	{
		/* (UTC+11:00) Solomon Is., New Caledonia */
		"Central Pacific Standard Time", "Central Pacific Daylight Time",
		"Pacific/Guadalcanal"
	},
	{
		/* (UTC-06:00) Central Time (US & Canada) */
		"Central Standard Time", "Central Daylight Time",
		"America/Chicago"
	},
	{
		/* (UTC-06:00) Guadalajara, Mexico City, Monterrey */
		"Central Standard Time (Mexico)", "Central Daylight Time (Mexico)",
		"America/Mexico_City"
	},
	{
		/* (UTC+12:45) Chatham Islands */
		"Chatham Islands Standard Time", "Chatham Islands Daylight Time",
		"Pacific/Chatham"
	},
	{
		/* (UTC+08:00) Beijing, Chongqing, Hong Kong, Urumqi */
		"China Standard Time", "China Daylight Time",
		"Asia/Shanghai"
	},
	{
		/* (UTC) Coordinated Universal Time */
		"Coordinated Universal Time", "Coordinated Universal Time",
		"UTC"
	},
	{
		/* (UTC-05:00) Havana */
		"Cuba Standard Time", "Cuba Daylight Time",
		"America/Havana"
	},
	{
		/* (UTC-12:00) International Date Line West */
		"Dateline Standard Time", "Dateline Daylight Time",
		"Etc/GMT+12"
	},
	{
		/* (UTC+03:00) Nairobi */
		"E. Africa Standard Time", "E. Africa Daylight Time",
		"Africa/Nairobi"
	},
	{
		/* (UTC+10:00) Brisbane */
		"E. Australia Standard Time", "E. Australia Daylight Time",
		"Australia/Brisbane"
	},
	{
		/* (UTC+02:00) Chisinau */
		"E. Europe Standard Time", "E. Europe Daylight Time",
		"Europe/Chisinau"
	},
	{
		/* (UTC-03:00) Brasilia */
		"E. South America Standard Time", "E. South America Daylight Time",
		"America/Sao_Paulo"
	},
	{
		/* (UTC-06:00) Easter Island */
		"Easter Island Standard Time", "Easter Island Daylight Time",
		"Pacific/Easter"
	},
	{
		/* (UTC-05:00) Eastern Time (US & Canada) */
		"Eastern Standard Time", "Eastern Daylight Time",
		"America/New_York"
	},
	{
		/* (UTC-05:00) Chetumal */
		"Eastern Standard Time (Mexico)", "Eastern Daylight Time (Mexico)",
		"America/Cancun"
	},
	{
		/* (UTC+02:00) Cairo */
		"Egypt Standard Time", "Egypt Daylight Time",
		"Africa/Cairo"
	},
	{
		/* (UTC+05:00) Ekaterinburg */
		"Ekaterinburg Standard Time", "Ekaterinburg Daylight Time",
		"Asia/Yekaterinburg"
	},
	{
		/* (UTC+12:00) Fiji */
		"Fiji Standard Time", "Fiji Daylight Time",
		"Pacific/Fiji"
	},
	{
		/* (UTC+02:00) Helsinki, Kyiv, Riga, Sofia, Tallinn, Vilnius */
		"FLE Standard Time", "FLE Daylight Time",
		"Europe/Kiev"
	},
	{
		/* (UTC+04:00) Tbilisi */
		"Georgian Standard Time", "Georgian Daylight Time",
		"Asia/Tbilisi"
	},
	{
		/* (UTC+00:00) Dublin, Edinburgh, Lisbon, London */
		"GMT Standard Time", "GMT Daylight Time",
		"Europe/London"
	},
	{
		/* (UTC-03:00) Greenland */
		"Greenland Standard Time", "Greenland Daylight Time",
		"America/Godthab"
	},
	{
		/*
		 * Windows uses this zone name in various places that lie near the
		 * prime meridian, but are not in the UK.  However, most people
		 * probably think that "Greenwich" means UK civil time, or maybe even
		 * straight-up UTC.  Atlantic/Reykjavik is a decent match for that
		 * interpretation because Iceland hasn't observed DST since 1968.
		 */
		/* (UTC+00:00) Monrovia, Reykjavik */
		"Greenwich Standard Time", "Greenwich Daylight Time",
		"Atlantic/Reykjavik"
	},
	{
		/* (UTC+02:00) Athens, Bucharest */
		"GTB Standard Time", "GTB Daylight Time",
		"Europe/Bucharest"
	},
	{
		/* (UTC-05:00) Haiti */
		"Haiti Standard Time", "Haiti Daylight Time",
		"America/Port-au-Prince"
	},
	{
		/* (UTC-10:00) Hawaii */
		"Hawaiian Standard Time", "Hawaiian Daylight Time",
		"Pacific/Honolulu"
	},
	{
		/* (UTC+05:30) Chennai, Kolkata, Mumbai, New Delhi */
		"India Standard Time", "India Daylight Time",
		"Asia/Calcutta"
	},
	{
		/* (UTC+03:30) Tehran */
		"Iran Standard Time", "Iran Daylight Time",
		"Asia/Tehran"
	},
	{
		/* (UTC+02:00) Jerusalem */
		"Israel Standard Time", "Israel Daylight Time",
		"Asia/Jerusalem"
	},
	{
		/* (UTC+02:00) Jerusalem (old spelling of zone name) */
		"Jerusalem Standard Time", "Jerusalem Daylight Time",
		"Asia/Jerusalem"
	},
	{
		/* (UTC+02:00) Amman */
		"Jordan Standard Time", "Jordan Daylight Time",
		"Asia/Amman"
	},
	{
		/* (UTC+02:00) Kaliningrad */
		"Kaliningrad Standard Time", "Kaliningrad Daylight Time",
		"Europe/Kaliningrad"
	},
	{
		/* (UTC+12:00) Petropavlovsk-Kamchatsky - Old */
		"Kamchatka Standard Time", "Kamchatka Daylight Time",
		"Asia/Kamchatka"
	},
	{
		/* (UTC+09:00) Seoul */
		"Korea Standard Time", "Korea Daylight Time",
		"Asia/Seoul"
	},
	{
		/* (UTC+02:00) Tripoli */
		"Libya Standard Time", "Libya Daylight Time",
		"Africa/Tripoli"
	},
	{
		/* (UTC+14:00) Kiritimati Island */
		"Line Islands Standard Time", "Line Islands Daylight Time",
		"Pacific/Kiritimati"
	},
	{
		/* (UTC+10:30) Lord Howe Island */
		"Lord Howe Standard Time", "Lord Howe Daylight Time",
		"Australia/Lord_Howe"
	},
	{
		/* (UTC+11:00) Magadan */
		"Magadan Standard Time", "Magadan Daylight Time",
		"Asia/Magadan"
	},
	{
		/* (UTC-03:00) Punta Arenas */
		"Magallanes Standard Time", "Magallanes Daylight Time",
		"America/Punta_Arenas"
	},
	{
		/* (UTC+08:00) Kuala Lumpur, Singapore */
		"Malay Peninsula Standard Time", "Malay Peninsula Daylight Time",
		"Asia/Kuala_Lumpur"
	},
	{
		/* (UTC-09:30) Marquesas Islands */
		"Marquesas Standard Time", "Marquesas Daylight Time",
		"Pacific/Marquesas"
	},
	{
		/* (UTC+04:00) Port Louis */
		"Mauritius Standard Time", "Mauritius Daylight Time",
		"Indian/Mauritius"
	},
	{
		/* (UTC-06:00) Guadalajara, Mexico City, Monterrey */
		"Mexico Standard Time", "Mexico Daylight Time",
		"America/Mexico_City"
	},
	{
		/* (UTC-07:00) Chihuahua, La Paz, Mazatlan */
		"Mexico Standard Time 2", "Mexico Daylight Time 2",
		"America/Chihuahua"
	},
	{
		/* (UTC-02:00) Mid-Atlantic - Old */
		"Mid-Atlantic Standard Time", "Mid-Atlantic Daylight Time",
		"Atlantic/South_Georgia"
	},
	{
		/* (UTC+02:00) Beirut */
		"Middle East Standard Time", "Middle East Daylight Time",
		"Asia/Beirut"
	},
	{
		/* (UTC-03:00) Montevideo */
		"Montevideo Standard Time", "Montevideo Daylight Time",
		"America/Montevideo"
	},
	{
		/* (UTC+01:00) Casablanca */
		"Morocco Standard Time", "Morocco Daylight Time",
		"Africa/Casablanca"
	},
	{
		/* (UTC-07:00) Mountain Time (US & Canada) */
		"Mountain Standard Time", "Mountain Daylight Time",
		"America/Denver"
	},
	{
		/* (UTC-07:00) Chihuahua, La Paz, Mazatlan */
		"Mountain Standard Time (Mexico)", "Mountain Daylight Time (Mexico)",
		"America/Chihuahua"
	},
	{
		/* (UTC+06:30) Yangon (Rangoon) */
		"Myanmar Standard Time", "Myanmar Daylight Time",
		"Asia/Rangoon"
	},
	{
		/* (UTC+07:00) Novosibirsk */
		"N. Central Asia Standard Time", "N. Central Asia Daylight Time",
		"Asia/Novosibirsk"
	},
	{
		/* (UTC+02:00) Windhoek */
		"Namibia Standard Time", "Namibia Daylight Time",
		"Africa/Windhoek"
	},
	{
		/* (UTC+05:45) Kathmandu */
		"Nepal Standard Time", "Nepal Daylight Time",
		"Asia/Katmandu"
	},
	{
		/* (UTC+12:00) Auckland, Wellington */
		"New Zealand Standard Time", "New Zealand Daylight Time",
		"Pacific/Auckland"
	},
	{
		/* (UTC-03:30) Newfoundland */
		"Newfoundland Standard Time", "Newfoundland Daylight Time",
		"America/St_Johns"
	},
	{
		/* (UTC+11:00) Norfolk Island */
		"Norfolk Standard Time", "Norfolk Daylight Time",
		"Pacific/Norfolk"
	},
	{
		/* (UTC+08:00) Irkutsk */
		"North Asia East Standard Time", "North Asia East Daylight Time",
		"Asia/Irkutsk"
	},
	{
		/* (UTC+07:00) Krasnoyarsk */
		"North Asia Standard Time", "North Asia Daylight Time",
		"Asia/Krasnoyarsk"
	},
	{
		/* (UTC+09:00) Pyongyang */
		"North Korea Standard Time", "North Korea Daylight Time",
		"Asia/Pyongyang"
	},
	{
		/* (UTC+07:00) Novosibirsk */
		"Novosibirsk Standard Time", "Novosibirsk Daylight Time",
		"Asia/Novosibirsk"
	},
	{
		/* (UTC+06:00) Omsk */
		"Omsk Standard Time", "Omsk Daylight Time",
		"Asia/Omsk"
	},
	{
		/* (UTC-04:00) Santiago */
		"Pacific SA Standard Time", "Pacific SA Daylight Time",
		"America/Santiago"
	},
	{
		/* (UTC-08:00) Pacific Time (US & Canada) */
		"Pacific Standard Time", "Pacific Daylight Time",
		"America/Los_Angeles"
	},
	{
		/* (UTC-08:00) Baja California */
		"Pacific Standard Time (Mexico)", "Pacific Daylight Time (Mexico)",
		"America/Tijuana"
	},
	{
		/* (UTC+05:00) Islamabad, Karachi */
		"Pakistan Standard Time", "Pakistan Daylight Time",
		"Asia/Karachi"
	},
	{
		/* (UTC-04:00) Asuncion */
		"Paraguay Standard Time", "Paraguay Daylight Time",
		"America/Asuncion"
	},
	{
		/* (UTC+05:00) Qyzylorda */
		"Qyzylorda Standard Time", "Qyzylorda Daylight Time",
		"Asia/Qyzylorda"
	},
	{
		/* (UTC+01:00) Brussels, Copenhagen, Madrid, Paris */
		"Romance Standard Time", "Romance Daylight Time",
		"Europe/Paris"
	},
	{
		/* (UTC+04:00) Izhevsk, Samara */
		"Russia Time Zone 3", "Russia Time Zone 3",
		"Europe/Samara"
	},
	{
		/* (UTC+11:00) Chokurdakh */
		"Russia Time Zone 10", "Russia Time Zone 10",
		"Asia/Srednekolymsk"
	},
	{
		/* (UTC+12:00) Anadyr, Petropavlovsk-Kamchatsky */
		"Russia Time Zone 11", "Russia Time Zone 11",
		"Asia/Kamchatka"
	},
	{
		/* (UTC+02:00) Kaliningrad */
		"Russia TZ 1 Standard Time", "Russia TZ 1 Daylight Time",
		"Europe/Kaliningrad"
	},
	{
		/* (UTC+03:00) Moscow, St. Petersburg */
		"Russia TZ 2 Standard Time", "Russia TZ 2 Daylight Time",
		"Europe/Moscow"
	},
	{
		/* (UTC+04:00) Izhevsk, Samara */
		"Russia TZ 3 Standard Time", "Russia TZ 3 Daylight Time",
		"Europe/Samara"
	},
	{
		/* (UTC+05:00) Ekaterinburg */
		"Russia TZ 4 Standard Time", "Russia TZ 4 Daylight Time",
		"Asia/Yekaterinburg"
	},
	{
		/* (UTC+06:00) Novosibirsk (RTZ 5) */
		"Russia TZ 5 Standard Time", "Russia TZ 5 Daylight Time",
		"Asia/Novosibirsk"
	},
	{
		/* (UTC+07:00) Krasnoyarsk */
		"Russia TZ 6 Standard Time", "Russia TZ 6 Daylight Time",
		"Asia/Krasnoyarsk"
	},
	{
		/* (UTC+08:00) Irkutsk */
		"Russia TZ 7 Standard Time", "Russia TZ 7 Daylight Time",
		"Asia/Irkutsk"
	},
	{
		/* (UTC+09:00) Yakutsk */
		"Russia TZ 8 Standard Time", "Russia TZ 8 Daylight Time",
		"Asia/Yakutsk"
	},
	{
		/* (UTC+10:00) Vladivostok */
		"Russia TZ 9 Standard Time", "Russia TZ 9 Daylight Time",
		"Asia/Vladivostok"
	},
	{
		/* (UTC+11:00) Chokurdakh */
		"Russia TZ 10 Standard Time", "Russia TZ 10 Daylight Time",
		"Asia/Magadan"
	},
	{
		/* (UTC+12:00) Anadyr, Petropavlovsk-Kamchatsky */
		"Russia TZ 11 Standard Time", "Russia TZ 11 Daylight Time",
		"Asia/Anadyr"
	},
	{
		/* (UTC+03:00) Moscow, St. Petersburg */
		"Russian Standard Time", "Russian Daylight Time",
		"Europe/Moscow"
	},
	{
		/* (UTC-03:00) Cayenne, Fortaleza */
		"SA Eastern Standard Time", "SA Eastern Daylight Time",
		"America/Cayenne"
	},
	{
		/* (UTC-05:00) Bogota, Lima, Quito, Rio Branco */
		"SA Pacific Standard Time", "SA Pacific Daylight Time",
		"America/Bogota"
	},
	{
		/* (UTC-04:00) Georgetown, La Paz, Manaus, San Juan */
		"SA Western Standard Time", "SA Western Daylight Time",
		"America/La_Paz"
	},
	{
		/* (UTC-03:00) Saint Pierre and Miquelon */
		"Saint Pierre Standard Time", "Saint Pierre Daylight Time",
		"America/Miquelon"
	},
	{
		/* (UTC+11:00) Sakhalin */
		"Sakhalin Standard Time", "Sakhalin Daylight Time",
		"Asia/Sakhalin"
	},
	{
		/* (UTC+13:00) Samoa */
		"Samoa Standard Time", "Samoa Daylight Time",
		"Pacific/Apia"
	},
	{
		/* (UTC+00:00) Sao Tome */
		"Sao Tome Standard Time", "Sao Tome Daylight Time",
		"Africa/Sao_Tome"
	},
	{
		/* (UTC+04:00) Saratov */
		"Saratov Standard Time", "Saratov Daylight Time",
		"Europe/Saratov"
	},
	{
		/* (UTC+07:00) Bangkok, Hanoi, Jakarta */
		"SE Asia Standard Time", "SE Asia Daylight Time",
		"Asia/Bangkok"
	},
	{
		/* (UTC+08:00) Kuala Lumpur, Singapore */
		"Singapore Standard Time", "Singapore Daylight Time",
		"Asia/Singapore"
	},
	{
		/* (UTC+02:00) Harare, Pretoria */
		"South Africa Standard Time", "South Africa Daylight Time",
		"Africa/Johannesburg"
	},
	{
		/* (UTC+02:00) Juba */
		"South Sudan Standard Time", "South Sudan Daylight Time",
		"Africa/Juba"
	},
	{
		/* (UTC+05:30) Sri Jayawardenepura */
		"Sri Lanka Standard Time", "Sri Lanka Daylight Time",
		"Asia/Colombo"
	},
	{
		/* (UTC+02:00) Khartoum */
		"Sudan Standard Time", "Sudan Daylight Time",
		"Africa/Khartoum"
	},
	{
		/* (UTC+02:00) Damascus */
		"Syria Standard Time", "Syria Daylight Time",
		"Asia/Damascus"
	},
	{
		/* (UTC+08:00) Taipei */
		"Taipei Standard Time", "Taipei Daylight Time",
		"Asia/Taipei"
	},
	{
		/* (UTC+10:00) Hobart */
		"Tasmania Standard Time", "Tasmania Daylight Time",
		"Australia/Hobart"
	},
	{
		/* (UTC-03:00) Araguaina */
		"Tocantins Standard Time", "Tocantins Daylight Time",
		"America/Araguaina"
	},
	{
		/* (UTC+09:00) Osaka, Sapporo, Tokyo */
		"Tokyo Standard Time", "Tokyo Daylight Time",
		"Asia/Tokyo"
	},
	{
		/* (UTC+07:00) Tomsk */
		"Tomsk Standard Time", "Tomsk Daylight Time",
		"Asia/Tomsk"
	},
	{
		/* (UTC+13:00) Nuku'alofa */
		"Tonga Standard Time", "Tonga Daylight Time",
		"Pacific/Tongatapu"
	},
	{
		/* (UTC+09:00) Chita */
		"Transbaikal Standard Time", "Transbaikal Daylight Time",
		"Asia/Chita"
	},
	{
		/* (UTC+03:00) Istanbul */
		"Turkey Standard Time", "Turkey Daylight Time",
		"Europe/Istanbul"
	},
	{
		/* (UTC-05:00) Turks and Caicos */
		"Turks And Caicos Standard Time", "Turks And Caicos Daylight Time",
		"America/Grand_Turk"
	},
	{
		/* (UTC+08:00) Ulaanbaatar */
		"Ulaanbaatar Standard Time", "Ulaanbaatar Daylight Time",
		"Asia/Ulaanbaatar"
	},
	{
		/* (UTC-05:00) Indiana (East) */
		"US Eastern Standard Time", "US Eastern Daylight Time",
		"America/Indianapolis"
	},
	{
		/* (UTC-07:00) Arizona */
		"US Mountain Standard Time", "US Mountain Daylight Time",
		"America/Phoenix"
	},
	{
		/* (UTC) Coordinated Universal Time */
		"UTC", "UTC",
		"UTC"
	},
	{
		/* (UTC+12:00) Coordinated Universal Time+12 */
		"UTC+12", "UTC+12",
		"Etc/GMT-12"
	},
	{
		/* (UTC+13:00) Coordinated Universal Time+13 */
		"UTC+13", "UTC+13",
		"Etc/GMT-13"
	},
	{
		/* (UTC-02:00) Coordinated Universal Time-02 */
		"UTC-02", "UTC-02",
		"Etc/GMT+2"
	},
	{
		/* (UTC-08:00) Coordinated Universal Time-08 */
		"UTC-08", "UTC-08",
		"Etc/GMT+8"
	},
	{
		/* (UTC-09:00) Coordinated Universal Time-09 */
		"UTC-09", "UTC-09",
		"Etc/GMT+9"
	},
	{
		/* (UTC-11:00) Coordinated Universal Time-11 */
		"UTC-11", "UTC-11",
		"Etc/GMT+11"
	},
	{
		/* (UTC-04:00) Caracas */
		"Venezuela Standard Time", "Venezuela Daylight Time",
		"America/Caracas"
	},
	{
		/* (UTC+10:00) Vladivostok */
		"Vladivostok Standard Time", "Vladivostok Daylight Time",
		"Asia/Vladivostok"
	},
	{
		/* (UTC+04:00) Volgograd */
		"Volgograd Standard Time", "Volgograd Daylight Time",
		"Europe/Volgograd"
	},
	{
		/* (UTC+08:00) Perth */
		"W. Australia Standard Time", "W. Australia Daylight Time",
		"Australia/Perth"
	},
	{
		/* (UTC+01:00) West Central Africa */
		"W. Central Africa Standard Time", "W. Central Africa Daylight Time",
		"Africa/Lagos"
	},
	{
		/* (UTC+01:00) Amsterdam, Berlin, Bern, Rome, Stockholm, Vienna */
		"W. Europe Standard Time", "W. Europe Daylight Time",
		"Europe/Berlin"
	},
	{
		/* (UTC+07:00) Hovd */
		"W. Mongolia Standard Time", "W. Mongolia Daylight Time",
		"Asia/Hovd"
	},
	{
		/* (UTC+05:00) Ashgabat, Tashkent */
		"West Asia Standard Time", "West Asia Daylight Time",
		"Asia/Tashkent"
	},
	{
		/* (UTC+02:00) Gaza, Hebron */
		"West Bank Gaza Standard Time", "West Bank Gaza Daylight Time",
		"Asia/Gaza"
	},
	{
		/* (UTC+02:00) Gaza, Hebron */
		"West Bank Standard Time", "West Bank Daylight Time",
		"Asia/Hebron"
	},
	{
		/* (UTC+10:00) Guam, Port Moresby */
		"West Pacific Standard Time", "West Pacific Daylight Time",
		"Pacific/Port_Moresby"
	},
	{
		/* (UTC+09:00) Yakutsk */
		"Yakutsk Standard Time", "Yakutsk Daylight Time",
		"Asia/Yakutsk"
	},
	{
		/* (UTC-07:00) Yukon */
		"Yukon Standard Time", "Yukon Daylight Time",
		"America/Whitehorse"
	},
	{
		NULL, NULL, NULL
	}
};
```
## Detailed Description
This function implements a recursive directory traversal algorithm to find the timezone file that best matches the system's timezone behavior. It operates by:

1. Scanning all entries in the current timezone directory using 
2. For each entry, determining if it's a subdirectory or timezone file
3. Recursively processing subdirectories to explore the full timezone hierarchy
4. For timezone files, calculating a match score using 
5. Maintaining the best match found so far, with tie-breaking logic based on timezone name preferences and alphabetical ordering

The function modifies the  buffer during traversal but restores it to its original state before returning, ensuring the caller's buffer remains intact.

## Parameters / Member Variables
- : Buffer of size MAXPGPATH containing the pathname of a directory with TZ files; modified internally but restored on exit
- : Points to the subfile name portion of tzdir (original directory name length + 1 for '/')
- : Pointer to tztry struct containing system timezone behavior data that needs to be matched
- : Pointer to integer holding the best match score found so far; updated if a better score is found
- : Buffer of length TZ_STRLEN_MAX + 1 containing the name of the best timezone found; updated with better matches

## Dependencies
- Functions called/Symbols referenced:
  - [pgfnames](../p/pgfnames.md): Get list of files in directory
  - S_ISDIR: Check if path is directory
  - [score_timezone](score_timezone.md): Calculate match score for timezone file
  - [zone_name_pref](../z/zone_name_pref.md): Get timezone name preference ranking
  - [strlcpy](strlcpy.md): Safe string copy
  - [pgfnames_cleanup](../p/pgfnames_cleanup.md): Clean up file list
  - TZ_STRLEN_MAX: Maximum timezone string length constant
- Called from:
  - [identify_system_timezone](../i/identify_system_timezone.md): Main timezone identification function
  - [scan_available_timezones](scan_available_timezones.md): Recursive calls for subdirectories

## Notes and Other Information
- Uses recursive directory traversal to explore the complete timezone database hierarchy
- Implements sophisticated tie-breaking logic when multiple timezones have equal scores, preferring zones with higher name preference rankings, shorter names, or lexicographically smaller names
- Includes debug output capability when DEBUG_IDENTIFY_TIMEZONE is defined
- Handles file system errors gracefully by continuing to process remaining entries
- Critical component of PostgreSQL's timezone auto-detection system during database initialization

## Simplified Source

```c
static void scan_available_timezones(char *tzdir, char *tzdirsub, struct tztry *tt,
                                    int *bestscore, char *bestzonename)
{
    int tzdir_orig_len = strlen(tzdir);
    char **names;
    char **namep;

    // Get list of files/directories in current timezone directory
    names = pgfnames(tzdir);
    if (!names)
        return;

    // Process each entry in the directory
    for (namep = names; *namep; namep++)
    {
        char *name = *namep;
        struct stat statbuf;

        // Skip hidden files (starting with '.')
        if (name[0] == '.')
            continue;

        // Build full path to current entry
        snprintf(tzdir + tzdir_orig_len, MAXPGPATH - tzdir_orig_len, "/%s", name);

        // Check if entry exists and get its type
        if (stat(tzdir, &statbuf) != 0)
        {
            tzdir[tzdir_orig_len] = '\0';  // Restore path
            continue;
        }

        if (S_ISDIR(statbuf.st_mode))
        {
            // Recursively scan subdirectory
            scan_available_timezones(tzdir, tzdirsub, tt, bestscore, bestzonename);
        }
        else
        {
            // Test this timezone file against system behavior
            int score = score_timezone(tzdirsub, tt);

            if (score > *bestscore)
            {
                // Found a better match
                *bestscore = score;
                strlcpy(bestzonename, tzdirsub, TZ_STRLEN_MAX + 1);
            }
            else if (score == *bestscore)
            {
                // Break ties using name preferences and length
                int namepref = zone_name_pref(tzdirsub) - zone_name_pref(bestzonename);

                if (namepref > 0 ||
                    (namepref == 0 &&
                     (strlen(tzdirsub) < strlen(bestzonename) ||
                      (strlen(tzdirsub) == strlen(bestzonename) &&
                       strcmp(tzdirsub, bestzonename) < 0))))
                {
                    strlcpy(bestzonename, tzdirsub, TZ_STRLEN_MAX + 1);
                }
            }
        }

        // Restore original path for next iteration
        tzdir[tzdir_orig_len] = '\0';
    }

    pgfnames_cleanup(names);
}
```