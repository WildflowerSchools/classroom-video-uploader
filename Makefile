TARGET ?= armhf
VERSION ?= 0

.PHONY: tmp-$(TARGET)/Dockerfile build build-all

build-all:
	make build TARGET=amd64 VERSION=$(VERSION)
	make build TARGET=armhf VERSION=$(VERSION)
	make tag VERSION=$(VERSION)

tag:
	docker manifest create wildflowerschools/classroom-video-uploader:v$(VERSION) wildflowerschools/classroom-video-uploader:amd64-v$(VERSION) --amend
	docker manifest create wildflowerschools/classroom-video-uploader:v$(VERSION) wildflowerschools/classroom-video-uploader:armhf-v$(VERSION) --amend
	docker manifest annotate wildflowerschools/classroom-video-uploader:v$(VERSION) wildflowerschools/classroom-video-uploader:armhf-v$(VERSION) --arch arm --variant v7 --os linux
	docker manifest push wildflowerschools/classroom-video-uploader:v$(VERSION)


build: tmp-$(TARGET)/Dockerfile
	docker build --no-cache -t wildflowerschools/classroom-video-uploader:$(TARGET)-v$(VERSION) tmp-$(TARGET)
	docker run wildflowerschools/classroom-video-uploader:$(TARGET)-v$(VERSION) uname -a
	docker push wildflowerschools/classroom-video-uploader:$(TARGET)-v$(VERSION)
	docker manifest create wildflowerschools/classroom-video-uploader:v$(VERSION) wildflowerschools/classroom-video-uploader:$(TARGET)-v$(VERSION) --amend

tmp-armhf/Dockerfile: Dockerfile $(shell find overlay-common overlay-armhf)
	rm -rf tmp-$(TARGET)
	mkdir tmp-$(TARGET) 
	cp Dockerfile.$(TARGET) $@
	cp -rf uploader tmp-$(TARGET)/
	cp -rf overlay-common tmp-$(TARGET)/
	cp -rf overlay-$(TARGET) tmp-$(TARGET)/
	sed -i 's/__DIGEST__/$(shell docker manifest inspect python:3.7-alpine | jq -r '.manifests[] | select(.platform.architecture == "arm") | select(.platform.variant == "v7") | .digest')/g' $@

tmp-amd64/Dockerfile: Dockerfile $(shell find overlay-common overlay-amd64)
	rm -rf tmp-amd64
	mkdir tmp-amd64
	cp Dockerfile.amd64 $@
	cp -rf uploader tmp-amd64/
	cp -rf overlay-common tmp-amd64/
	cp -rf overlay-$(TARGET) tmp-amd64/
