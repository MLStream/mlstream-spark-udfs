# Handy Spark Tools Accessible From Spark SQL

Spark and PySpark packages that extend Apache Spark (UDFs) with additional SQL functions.
Available for both Scala and Python.

[![Build Status](https://github.com/MLStream/mlstream-spark-udfs/workflows/build/badge.svg?branch=master)](https://github.com/MLStream/mlstream-spark-udfs/actions)

# For Data Scientists and Engineers using Spark

Objectives:

- "As a data scientist, I can get more done by using SQL functions" 

- "As a data engineer, I can get real-time aggregations via SQL functions"

# Available Functions

## Generic Extensions

| UDF Function  | Description   | Example | Status |
| ------------- | ------------- | --------| -------|
| last_k        | Returns the lask k occurences  | SELECT user_id, last_k(page_id, timestamp, 100, "unique") FROM dataset | In Development |
| approx_topk   |  Returns the most frequent items using a fast approximation algorithm with limited memory   | SELECT approx_topk(ip_address, 1000, "10MB") FROM dataset | Available |
| approx_cond_topk   |  Returns the most frequent items conditioned on anyother item using a fast approximation algorithm with limited memory   | NA | In Development |

# Getting Started

## Start with a Prepared Docker Image with PySpark and Jupyter


1. Browse the [Jupyter notebooks](https://github.com/MLStream/mlstream-spark-udfs/tree/master/notebooks).

2. Clone the repository

```
git clone git@github.com:MLStream/mlstream-spark-udfs.git
```

3. Run the demo (Linux and Mac only)

The following command starts a demo Jupyter server which is ready to use with local files.


```
./demo.sh
```

# Install via Python pip or as a Spark Package

TODO

# Disclaimer

The project [*mlstream-spark-udfs*](https://github.com/MLStream/mlstream-spark-udfs) is distributed in the hope it will be useful and help you solve pressing problems.
At the same time its still early days for *mlstream-spark-udfs*. *mlstream-spark-udfs* may contain many bugs - known or
unknown, it may crash, force yor computer to run out of memory and produce erroneous results. Please carry out
due diligence before using and deploying in your organization. The developers developers of *mlstream-spark-udfs*, be they
organizations or people, should not be held liable
for any damages which result from running the code.
The code is distributed under [Apache License](LICENSE) which should be consulted for warranties and liabilities.
This disclaimer to does not replace the license.
 
# License

The code is distributed under [Apache License](LICENSE). Please check the source files in the repositories for third-party libraries used.
We further use Source code derived from [GoLang sort](https://golang.org/src/sort/sort.go). Please consult [GoLang LICENSE](https://golang.org/LICENSE)

# Looking for Help with Spark and Spark Extensions for Your Organization

# Code of Conduct

TODO

# Support

Please file an issue or contact hello@mlstream.com.



