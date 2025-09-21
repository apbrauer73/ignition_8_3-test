SELECT 
	audit_events_id, 
	DATETIME(ROUND(event_timestamp / 1000), 'unixepoch') as event_timestamp,
	actor, 
	action,
	action_target,
	action_value,
	actor_host,
	status_code,
	originating_system,
	originating_context
FROM 
	AUDIT_EVENTS
WHERE 
	event_timestamp BETWEEN :startDate AND :endDate
ORDER BY 
	audit_events_id DESC
LIMIT 3000