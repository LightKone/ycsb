all: docker_build docker_push

DOCKERREPONAME := dvasilas/ycsb
TAG := $(shell git log -1 --pretty=%H | cut -c1-8)
IMG := ${DOCKERREPONAME}:${TAG}

docker_build:
	echo ${TAG}
	echo ${IMG}
	docker build -t proteus:local .
	docker tag proteus:local ${IMG}

docker_push:
	docker push ${IMG}

.PHONY: docker_build docker_push

