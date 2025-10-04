def onTagChange(initialChange, newValue, previousValue, event, executionCount):
	if not initialChange:
		system.project.requestScan(5)