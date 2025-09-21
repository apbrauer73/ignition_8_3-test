SELECT
	displaypath,
	datetime(eventtime / 1000, 'unixepoch') as eventtime
FROM
	alarm_events
ORDER BY
	eventtime DESC
LIMIT 5