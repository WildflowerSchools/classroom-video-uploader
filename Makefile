VERSION ?= 64

.PHONY: build


lint-app:
	@pylint uploader


build: lint-app
	docker buildx create --name multiarch
	docker buildx use multiarch
	docker buildx build -t wildflowerschools/classroom-video-uploader:v${VERSION} --platform linux/amd64,linux/arm64 -f Dockerfile --push .
	docker buildx rm multiarch
