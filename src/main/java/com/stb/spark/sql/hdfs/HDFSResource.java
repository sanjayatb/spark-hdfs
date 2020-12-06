package com.stb.spark.sql.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

/***
 * HDFSResource class
 */
public class HDFSResource implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSResource.class);

    private final Configuration configuration;
    private String path;
    private FileSystem fileSystem;

    public HDFSResource() {
        configuration = new Configuration();
        configuration.setBoolean("fs.hdfs.impl.disable.cache",true);
    }

    public Path getResolvePath() {
        try {
            return fileSystem.resolvePath(new Path(path));
        } catch (IOException e) {
            LOGGER.error("Fail resolve Path");
        }
        return null;
    }

    public FileSystem getFileSystem(String path) throws IOException {
        this.path = path;
        this.fileSystem = new Path(path).getFileSystem(configuration);
        return fileSystem;
    }

    public FileSystem getFileSystem() {
        try {
            this.fileSystem = FileSystem.get(configuration);
        } catch (IOException e) {
            LOGGER.error("Fail to get file system");
        }
        return fileSystem;
    }
}
