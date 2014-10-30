PREFIX := /opt/mistify
SBIN_DIR=$(PREFIX)/sbin

cmd/mistify-agent-image/mistify-agent-image: cmd/mistify-agent-image/main.go
	cd cmd/mistify-agent-image && \
	go get && \
	go build


clean:
	cd cmd/mistify-agent-image && \
	go clean

install: cmd/mistify-agent-image/mistify-agent-image
	install -D cmd/mistify-agent-image/mistify-agent-image $(DESTDIR)$(SBIN_DIR)/mistify-agent-image

