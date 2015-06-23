/*
mistify-agent-image runs the subagent and HTTP API.

Usage

The following arguments are understood:

	Usage of ./mistify-agent-image:
	-i, --image-service="image.services.lochness.local": image service. srv query used to find port if not specified
	-l, --log-level="warning": log level: debug/info/warning/error/critical/fatal
	-p, --port=19999: listen port
	-z, --zpool="mistify": zpool
*/
package main
