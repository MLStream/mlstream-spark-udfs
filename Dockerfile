FROM alpine:3.10

# Java Version: Open JDK 8
# SBT Version: 1.2.8
# Scala Version: 2.11.12
# Spark Version: 2.4.4
# Python Version: 3.7

RUN apk add --no-cache curl bash openjdk8-jre python3 py3-pip make && \
	python3 -m pip install pyspark setuptools && \
	ln -s /usr/bin/python3 /usr/bin/python

RUN wget https://piccolo.link/sbt-1.2.8.tgz && \
	tar -xzvf sbt-1.2.8.tgz && \
	/sbt/bin/sbt version

ENV PATH=/sbt/bin:$PATH

WORKDIR /spark-udfs-tmp
COPY spark/build.sbt ./
COPY spark/project/build.properties spark/project/plugins.sbt project/
RUN sbt update

WORKDIR /spark-udfs

ENTRYPOINT ["/bin/bash"]

