copy_jars:
	cd ../spark/ && sbt clean package
	cp ../spark/target/scala-2.11/mlstream-spark-udfs_*.jar jars/

package: copy_jars integ_test
	python setup.py sdist

integ_test: copy_jars
	./integ-test.sh

