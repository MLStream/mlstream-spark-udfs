docker build . -f Dockerfile.develop -t mlstream-udfs-develop
	
docker run --rm -it \
	--name spark_udfs_develop \
	--cpus 2 \
	--memory 6GB \
	-u 502:502 \
	-p 8888:8888 \
        -p 4040:4040 \
	--workdir /home/docker/spark-udfs -v "`pwd`":"/home/docker/spark-udfs" mlstream-udfs-develop \
	jupyter notebook
