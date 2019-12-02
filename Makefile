all: docker_build docker_push

DOCKERREPONAME := dvasilas/ycsb
TAG := $(shell git log -1 --pretty=%H | cut -c1-8)
IMG := ${DOCKERREPONAME}:${TAG}
IMGPARSE := ${DOCKERREPONAME}:${TAG}_parse

docker_build:
	echo ${TAG}
	echo ${IMG}
	docker build -t ycsb:local .
	docker build -f Dockerfile_parse -t ycsb:parse .
	docker tag ycsb:local ${IMG}
	docker tag ycsb:parse ${IMGPARSE}

docker_push:
	docker push ${IMG}
	docker push ${IMGPARSE}

.PHONY: docker_build docker_push

