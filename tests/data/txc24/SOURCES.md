# Test fixture sources

All files are real TransXChange documents; they are used unmodified except where noted.

## SVRABAO001.xml

TransXChange 2.5, First Aberdeen service ABAO001 (Traveline National Dataset style).
Attached by @nadiiia to https://github.com/HTenkanen/transx2gtfs/issues/29 (SVRABAO001.zip, 2020-04-28).

## Bus Open Data Service files

TransXChange 2.4 files from the Bus Open Data Service bulk timetable archive
(https://data.bus-data.dft.gov.uk/timetable/download/bulk_archive, file
bodds_archive_20260825.zip, downloaded 2026-08-26), published under the Open Government
Licence v3.0 (https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).
`Track` elements (route geometry, not read by transx2gtfs) were removed from two files to
reduce their size; nothing else was changed.

### LGEN_403_LGENPK000181698403_20251025_-_2197827.xml

- Archive member: `Go-Ahead Group plc_10/13649_123786_2026-08-25_03-01-10_current.zip` → `LGEN_403_LGENPK000181698403_20251025_-_2197827.xml`
- Operator: Go-Ahead London (NationalOperatorCode LGEN)
- Original size 382906 bytes

### HRCS2_HRCS_240_PK20556564_20260831_20260730_114905.xml

- Archive member: `HADLOW RURAL COMMUNITY SCHOOL LIMITED_985/24640_121997_2026-07-30_10-51-59.zip` → `HRCS2_HRCS_240_PK20556564_20260831_20260730_114905.xml`
- Operator: Hadlow Rural Comm. Sch. (NationalOperatorCode HRCS)
- Original size 427031 bytes, 53 `Track` elements removed (70691 bytes)

### FWC001_FWAY_150_PF000080424_20260901_20260714_210137.xml

- Archive member: `Fourways Coaches_351/24373_120700_2026-07-14_20-18-49_FWAY – Fourways Coaches – Sep 2026.zip` → `FWC001_FWAY_150_PF000080424_20260901_20260714_210137.xml`
- Operator: Fourways (NationalOperatorCode FWAY)
- Original size 418380 bytes, 26 `Track` elements removed (63174 bytes)
