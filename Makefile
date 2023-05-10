VERSION ?= 69

.PHONY: build


lint-app:
	@pylint uploader


build: lint-app
	docker buildx create --name uploader-builds --node "v${VERSION}" || true
	docker buildx use uploader-builds
	docker buildx build -t wildflowerschools/classroom-video-uploader:v${VERSION} --platform linux/amd64 -f Dockerfile --push .
