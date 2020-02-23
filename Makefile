# make variables containing the user and group id that need to be passed to docker_develop

UID := $(shell id -u "${USER}")
GID := $(shell id -g "${USER}")

docker_build:
	docker build . -t mlstream-udfs

docker_run_bash: docker_build
	docker run -it --workdir /github/workspace -v "`pwd`":"/github/workspace" mlstream-udfs  -c "/bin/bash"

docker_run_spark_test: docker_build
	docker run -it --workdir /github/workspace -v "`pwd`":"/github/workspace" mlstream-udfs  -c "cd spark && sbt test"

docker_run_pyspark_test: docker_build
	docker run -it --workdir /github/workspace -v "`pwd`":"/github/workspace" mlstream-udfs  -c "cd pyspark && make integ_test"

docker_run_tests: docker_run_spark_test docker_run_pyspark_test

docker_build_develop:
	docker build . -f Dockerfile.develop -t mlstream-udfs-develop

docker_develop: docker_build_develop
	docker run --rm -it \
	--name spark_udfs_develop \
	--cpus 4 \
	--memory 6GB \
	-u $(UID):$(GID) \
	-p 8888:8888 \
        -p 4040:4040 \
	--workdir /home/docker/spark-udfs -v "`pwd`":"/home/docker/spark-udfs" mlstream-udfs-develop \
	jupyter notebook

