# DANE calendar fixtures

`2026-annual-excerpt.html` preserves the four event tables for November 4 exports,
November 9 IPC, November 17 ISE and November 30 GEIH from the official annual
list fetched October 30, 2026:
https://www.dane.gov.co/index.php/calendario/year.listevents/2026/10/30/-?limit=0

`2026-navigation-excerpt.html` preserves the annual-list link advertised by
https://www.dane.gov.co/index.php/calendario on the same date.

Unrelated rows, navigation, scripts and page chrome are removed. Clock text,
event URLs and table structure are preserved. Tests construct separate synthetic
failure, conflict and year-boundary cases; they do not represent official events.
