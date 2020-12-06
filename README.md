# spark-hdfs
HDFS file read spark custom source for structured streaming

This library is custom spark source to read files in hdfs folder

### Example
```
        SparkSession _sparkSession = SparkSession
                        .builder()
                        .appName("HDFS Spark application")
                        .master("local[*]")
                        .getOrCreate();

                Dataset<?> input = _sparkSession.readStream()
                        .format("hdfs")
                        .option("path", "hdfs.folder.path")
                        .option("fileFormat", "XML")
                        .option("split", "xmlArraySplitTag")
                        .load();
```