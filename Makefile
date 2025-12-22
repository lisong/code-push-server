ROOT := $(shell pwd)
VERSION := $(shell node -p "require('./package.json').version")

.PHONY: test
test:
	@echo "\nRunning integration tests..."
	@mocha tests/api/init --exit
	@mocha tests/api/users tests/api/auth tests/api/account tests/api/accessKeys tests/api/apps tests/api/index --exit --recursive --timeout 30000

.PHONY: coverage
coverage:
	@echo "\nCheck test coverage..."
	@mocha tests/api/init --exit
	@nyc mocha tests/api/users tests/api/auth tests/api/account tests/api/accessKeys tests/api/apps tests/api/index --exit --recursive --timeout 30000

.PHONY: release-docker
release-docker:
	@echo "\nBuilding docker image..."
	docker pull node:lts-alpine
	docker build --build-arg VERSION=${VERSION} -t shmopen/code-push-server:latest --no-cache .
	docker tag shmopen/code-push-server:latest shmopen/code-push-server:${VERSION}
	docker push shmopen/code-push-server:${VERSION}
	docker push shmopen/code-push-server:latest
