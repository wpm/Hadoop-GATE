Hadoop GATE
===========

This project contains a simple Hadoop job that runs a [GATE](http://gate.ac.uk/ "GATE") application.

This job runs an archived GATE application on text files comprised of one document per line. It produces sequence files
containing XML representations of the document annotation. The GATE application is a archive file with an
application .xgapp file in its root directory. This application is copied to HDFS and placed into the distributed
cache.

The first positional argument is the GATE application. Subsequent positional arguments are input directories,
except for the final positional argument, which is the output directory. A sample command might be

	hadoop jar Hadoop-GATE-1.0.jar ANNIE.zip input output

This project uses the new Hadoop API. It is built with Maven and demonstrates how to use Maven to package all the
GATE dependencies into a single jar file.

The [Behemoth](https://github.com/jnioche/behemoth) project also runs GATE on Hadoop.