package com.inforefiner.eventgateway;

import com.merce.woven.common.workflow.sdk.ProcessDelegate;
import com.merce.woven.common.workflow.sdk.ProcessOutCollector;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by joey on 2019/1/22 3:02 PM.
 */
public class TriggerDataflow implements ProcessDelegate {

    private Logger logger = LoggerFactory.getLogger(TriggerDataflow.class);

    private FileSystem fs;

    private String filePath;

    private String lastEmit;

    public void prepare(Properties props, Map<String, Object> context) {
        logger.info("prepare props = {}, context = {}", props, context);
        Configuration conf = new Configuration();
        String USER_NAME = System.getenv("HADOOP_USER_NAME");
        if (StringUtils.isBlank(USER_NAME)) {
            USER_NAME = "hdfs";
        }
        String CONF_PATH = System.getenv("HADOOP_CONF_DIR");
        logger.info("CONF_PATH = {}", CONF_PATH);
        if (StringUtils.isNotBlank(CONF_PATH)) {
            conf.addResource(new Path(CONF_PATH + File.separator + "core-site.xml"));
            conf.addResource(new Path(CONF_PATH + File.separator + "hdfs-site.xml"));
        }
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            logger.error("can't create hdfs FileSystem", e);
        }
        if (props != null) {
            if (props.getProperty("filePath") != null) {
                filePath = props.getProperty("filePath");
                logger.info("use filePath = " + filePath);
            }
            if (props.getProperty("lastEmit") != null) {
                lastEmit = props.getProperty("lastEmit");
                logger.info("use lastEmit = " + lastEmit);
            }
        }
    }

    public void execute(Map<String, Map<String, List<String>>> map, ProcessOutCollector collector) {
        try {
            Path path = new Path(filePath);
            boolean exists = fs.exists(path);
            logger.info("TriggerDataflow execute, filePath = {}, exists = {}", filePath, exists);
            FileStatus[] dirs = fs.listStatus(path);
            List<String> list = new ArrayList();
            for (FileStatus status : dirs) {
                list.add(status.getPath().getName());
            }
            logger.info("TriggerDataflow execute, filePath = {}, fileList = {}", filePath, list);
            if (list.size() > 0) {
                Collections.sort(list);
                int start = 0;
                if (lastEmit != null) {
                    start = list.indexOf(lastEmit);
                    start = start + 1;
                }
                int end = list.size() - 1; // keep last one
                logger.info("TriggerDataflow execute, filePath = {}, fileList = {}, start = {}, end = {}", filePath, list, start, end);
                if (end > start) {
                    List<String> toEmit = list.subList(start, end);
                    String[] arr = new String[toEmit.size()];
                    for (int i = 0; i < arr.length; i += 1) {
                        arr[i] = filePath + "/" + toEmit.get(i);
                    }
                    Map<String, Object> params = new HashMap();
                    params.put("eventStepInputPath", StringUtils.join(arr, ","));
                    logger.info("TriggerDataflow executing, filePath = {}, params = {}", filePath, params);
                    collector.emit(params, map);
                    lastEmit = toEmit.get(toEmit.size() - 1);
                    logger.info("TriggerDataflow execute done, filePath = {}, lastEmit = {}", filePath, lastEmit);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void cleanup() {

    }

    public static void main(String[] args) {
        List list = Arrays.asList("1", "2", "3", "4", "5");
        List subList = list.subList(1, list.size());
        System.out.println(subList.get(subList.size()));
//        Configuration conf = new Configuration();
//        String USER_NAME = System.getenv("HADOOP_USER_NAME");
//        if (StringUtils.isBlank(USER_NAME)) {
//            USER_NAME = "hdfs";
//        }
//        String CONF_PATH = System.getenv("HADOOP_CONF_DIR");
//        if (StringUtils.isNotBlank(CONF_PATH)) {
//            conf.addResource(new Path(CONF_PATH + File.separator + "core-site.xml"));
//            conf.addResource(new Path(CONF_PATH + File.separator + "hdfs-site.xml"));
//        }
//        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
//        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//        try {
//            FileSystem fs = FileSystem.get(conf);
//            Path path = new Path("/tmp/collecter/c1/oracle_1214/ORACLE_ORACLE1_oracle_1214");
//            System.out.println(fs.exists(path));
//            FileStatus[] list = fs.listStatus(path);
//            for (FileStatus status : list) {
//                System.out.println(status.getPath().getName());
//            }
//            RemoteIterator<LocatedFileStatus> iterator = fs.list
//            List<String> list = new ArrayList();
//            while (iterator.hasNext()) {
//                LocatedFileStatus fileStatus = iterator.next();
//                String fileName = fileStatus.getPath().getName();
//                System.out.println(fileName);
//            }
//        } catch (IOException e) {
//        }
//        System.out.println("asd");
    }
}
