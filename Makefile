VERSION ?= 0

.PHONY: build


build:
	docker buildx create --name multiarch
	docker buildx use multiarch
	docker buildx build -t wildflowerschools/classroom-video-uploader:v${VERSION} --platform linux/amd64,linux/arm64,linux/arm/v7 -f Dockerfile --push .
	docker buildx rm multiarch
