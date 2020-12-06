package com.stb.spark.sql.hdfs;

import com.stb.spark.sql.hdfs.iterators.IteratorFactory;
import com.stb.spark.sql.hdfs.iterators.MultiIterator;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.stb.spark.sql.hdfs.Constants.EXCEL_FORMAT;
import static com.stb.spark.sql.hdfs.Constants.FILE_FORMAT;

/***
 * RDD creation class
 */
public class HDFSSourceRDD extends RDD<byte[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSSourceRDD.class);
    public static final StructType SCHEMA = new StructType().add("value", DataTypes.BinaryType);
    private static final ClassTag<byte[]> EVIDENCE = ClassTag$.MODULE$.apply(byte[].class);
    private static final List<Dependency<?>> EMPTY_DEPENDENCY = new ArrayList<>();
    private final Map<String, String> options;
    private final String pathStr;
    private final String fileFormat;

    public HDFSSourceRDD(SQLContext context, Map<String, String> options) {
        super(context.sparkContext(), JavaConversions.asScalaIterator(EMPTY_DEPENDENCY.iterator()).toSeq(), EVIDENCE);
        this.options = options;
        pathStr = options.get("path");
        fileFormat = options.get(FILE_FORMAT);
    }

    public Iterator<byte[]> compute(Partition partition, TaskContext context) {
        MultiIterator<byte[]> multiIterator = new MultiIterator<>();

        if(StringUtils.isBlank(fileFormat)){
            return JavaConversions.asScalaIterator(multiIterator);
        }

        try{
            HDFSResource hdfsResource = new HDFSResource();
            FileSystem fileSystem = hdfsResource.getFileSystem(pathStr);
            Path filePath = hdfsResource.getResolvePath();

            FileStatus[] fileStatuses = fileSystem.listStatus(filePath);

            Arrays.stream(fileStatuses).filter(FileStatus::isFile).forEach( fileStatus -> {
                String extention = StringUtils.upperCase(FilenameUtils.getExtension(fileStatus.getPath().toString())).trim();
                if(!"_COPYING_".equals(extention)){
                   String fileToRead = fileStatus.getPath().toString();
                   try{
                       InputStream in = creteInputStream(fileSystem,fileStatus,partition);
                       if (null!=in){
                           java.util.Iterator<byte[]> msgIterator = IteratorFactory.create(fileFormat,in,options,fileToRead);
                           multiIterator.add(msgIterator);
                       }
                   }catch (Exception e){
                       LOGGER.error("Fail to read",e);
                   }
                }
            });
        } catch (IOException e) {
            LOGGER.error("Fail to read message from queue");
        }
        return JavaConversions.asScalaIterator(multiIterator);
    }

    private InputStream creteInputStream(FileSystem fileSystem, FileStatus fileStatus, Partition partition) throws IOException {
        if(partition.index() !=0 && EXCEL_FORMAT.equals(fileFormat)){
            return null;
        }
        return fileSystem.open(fileStatus.getPath());
    }

    public Partition[] getPartitions() {
        AtomicInteger partitionCounter = new AtomicInteger(0);
        List<String> executors = JavaConversions.seqAsJavaList(super.context().getExecutorIds());
        if (executors.isEmpty()) {
            executors = Arrays.asList("driver");
        }
        return executors.stream().map(
                ex -> new HDFSSourceRDDPartition(partitionCounter.getAndIncrement()))
                .collect(Collectors.toList()).toArray(new HDFSSourceRDDPartition[]{});
    }

    private static final class HDFSSourceRDDPartition implements Partition {
        private final int index;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HDFSSourceRDDPartition that = (HDFSSourceRDDPartition) o;
            return index == that.index;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index);
        }

        public HDFSSourceRDDPartition(int index) {
            this.index = index;
        }

        @Override
        public int index() {
            return index;
        }
    }

}
