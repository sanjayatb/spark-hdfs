package com.stb.spark.sql.hdfs;

import com.databricks.spark.xml.XmlInputFormat;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Option;
import scala.Serializable;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.stb.spark.sql.hdfs.Constants.FILE_FORMAT;
import static com.stb.spark.sql.hdfs.Constants.XML_FORMAT;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE;

/**
 * HDFSSource implements the spark source interface
 */
public class HDFSSource implements Source {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSSource.class);

    private static final ClassTag<InternalRow> EVIDENCE = ClassTag$.MODULE$.apply(InternalRow.class);

    private final SQLContext sqlContext;
    private final HDFSResource resource;
    private final Map<String, String> options;
    private final FileSystem fileSystem;
    private final String pathStr;
    private final Path filePath;
    private final String fileFormat;
    private final String splitTag;
    private final int partitionSliceMb;

    private final AtomicLong currentOffset = new AtomicLong(0);

    public HDFSSource(SQLContext sqlContext, scala.collection.immutable.Map<String, String> optionsScala) {
        this.sqlContext = sqlContext;
        this.options = (Map<String, String>) JavaConverters.mapAsJavaMapConverter(optionsScala);
        this.resource = new HDFSResource();
        pathStr = options.get("path");
        fileFormat = options.get(FILE_FORMAT);
        splitTag = options.get("split");
        partitionSliceMb = Integer.parseInt(options.getOrDefault("partitionSliceMb","15"));
        try {
            fileSystem = resource.getFileSystem(pathStr);
            filePath = resource.getResolvePath();
            LOGGER.info("Start stream on {} path",pathStr);
        } catch (IOException e) {
            throw new RuntimeException("Fail to find path for HDFS resource");
        }
    }

    @Override
    public StructType schema() {
        return HDFSSourceRDD.SCHEMA;
    }

    @Override
    public Option<Offset> getOffset() {
        FileStatus[] fileStatuses;
        long current = currentOffset.get();
        try {
            fileStatuses = fileSystem.listStatus(filePath);
        } catch (IOException e) {
            throw new RuntimeException("Unable to get the offset for HDFS source path "+pathStr,e);
        }

        AtomicReference<HDFSSourceOffset> offset = new AtomicReference<>();
        offset.set(null);

        Arrays.stream(fileStatuses).filter(FileStatus::isFile)
                .forEach(fileStatus -> {
                    String extention = StringUtils.upperCase(FilenameUtils.getExtension(fileStatus.getPath().toString())).trim();
                    if(!"_COPYING_".equals(extention)){
                        currentOffset.compareAndSet(current,System.currentTimeMillis());
                        offset.set(new HDFSSourceOffset(currentOffset.get()));
                    }
                });

        return Option.apply(offset.get());
    }

    @Override
    public Dataset<Row> getBatch(Option<Offset> start, Offset end) {
        if (end.json().equals(String.valueOf(currentOffset.get()))) {
            if( XML_FORMAT.equals(fileFormat)){
                Configuration hconf = new Configuration();
                String xmlStartTag = "<"+splitTag+">";
                String xmlEndTag = "</"+splitTag+">";
                hconf.setLong(SPLIT_MAXSIZE,partitionSliceMb*1024L*1024L);
                hconf.set(XmlInputFormat.START_TAG_KEY(),xmlStartTag);
                hconf.set(XmlInputFormat.END_TAG_KEY(),xmlEndTag);

                RDD<Tuple2<LongWritable,Text>> rdd =
                        sqlContext.sparkContext().newAPIHadoopFile(pathStr,XmlInputFormat.class,LongWritable.class,Text.class,hconf);
                Function1<Tuple2<LongWritable,Text>,InternalRow> function1 = new TupleFunction1();
                RDD<InternalRow> internalRowRDD = rdd.map(function1,EVIDENCE);
                return sqlContext.internalCreateDataFrame(internalRowRDD,schema(),true);
            }else {
                HDFSSourceRDD result = new HDFSSourceRDD(sqlContext,options);
                Function1<byte[], InternalRow> function = new ByteFunctionWrapper();
                RDD<InternalRow> rowRDD = result.map(function,EVIDENCE);
                return sqlContext.internalCreateDataFrame(rowRDD,schema(),true);
            }

        } else {
            return sqlContext.internalCreateDataFrame(sqlContext.sparkContext().emptyRDD(EVIDENCE), schema(), true);
        }
    }

    public void commit(Offset end) {
    }

    public void stop() {
        try {
            fileSystem.close();
        } catch (IOException e) {
            LOGGER.error("Fail to close",e);
        }
    }

    private static class TupleFunction1 extends AbstractFunction1<scala.Tuple2<LongWritable, Text>, InternalRow> implements Serializable {
        @Override
        public InternalRow apply(scala.Tuple2<LongWritable, Text> tuple) {
            Seq<Object> result = JavaConversions.asScalaBuffer(Arrays.asList((Object) tuple._2.toString().getBytes())).toSeq();
            return InternalRow.apply(result);
        }
    }

    private static class ByteFunctionWrapper extends AbstractFunction1<byte[], InternalRow> implements Serializable {
        @Override
        public InternalRow apply(byte[] bytes) {
            Seq<Object> result = JavaConversions.asScalaBuffer(Arrays.asList((Object) bytes)).toSeq();
            return InternalRow.apply(result);
        }
    }

}
