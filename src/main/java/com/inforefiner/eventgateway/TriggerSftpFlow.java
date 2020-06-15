package com.inforefiner.eventgateway;

import com.inforefiner.eventgateway.utils.SftpUtil;
import com.jcraft.jsch.ChannelSftp;
import com.merce.woven.common.workflow.sdk.ProcessDelegate;
import com.merce.woven.common.workflow.sdk.ProcessOutCollector;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class TriggerSftpFlow
        implements ProcessDelegate
{
    private Logger logger;

    private String host;
    private String port;
    private String filePath;// /home/sftp/MME/


    private String user;
    private String password;
    private String pathPrefix;
    private String dateFormat;//yyyyMMdd, yyyy-MM-dd
    private String dateFunction;//today,yesterday
    private String pathSuffix;//.csv
    private Integer lastEmit;//标记最后文件的index

    public TriggerSftpFlow()
    {
        this.logger = LoggerFactory.getLogger(TriggerSftpFlow.class);
    }

    public void prepare(Properties props, Map<String, Object> context)
    {
        this.logger.info("prepare props = {}, context = {}", props, context);

        this.user = props.getProperty("user");
        this.password = props.getProperty("password");

        this.pathPrefix = props.getProperty("pathPrefix"); //sftp://info2/home/merce/

        String path = pathPrefix;
        if (pathPrefix.startsWith("sftp://")) {
            path = path.substring("sftp://".length());
        }
        StringBuilder bf = new StringBuilder();
        String[] strArr = path.split("/");
        String address = strArr[0];
        if(StringUtils.isEmpty(this.host) && StringUtils.isEmpty(this.port)) {
            if (address.indexOf(":") >= 0) {
                String[] hostPot = address.split(":");
                this.host = hostPot[0];
                this.port = hostPot[1];
            } else {
                this.host = address;
                this.port = "22";
            }
        }
        for (int i = 1; i < strArr.length; i++) {
            bf.append("/" + strArr[i]);
        }
        this.filePath = bf.toString();

        this.dateFormat = props.getProperty("dateFormat");
        this.dateFunction = props.getProperty("dateFunction");
        this.pathSuffix = props.getProperty("pathSuffix");
        if(props.getProperty("lastEmit") != null){
            lastEmit = Integer.valueOf(props.getProperty("lastEmit"));
        }else {
            lastEmit = -1;
        }
        logger.info("host {}, port {}, filepath {}, dateFormat {}, dateFunction {}, pathSuffix {}", host, port, filePath, dateFormat, dateFunction, pathSuffix);
    }

    public void execute(Map<String, Map<String, List<String>>> map, ProcessOutCollector collector) {
        try {
            Calendar calendar = Calendar.getInstance();
            String timeStr = null;
            String path = null;
            Map<ChannelSftp.LsEntry,String> finalPaths = new HashMap<ChannelSftp.LsEntry,String>();
            if("today".equals(this.dateFunction)){
                timeStr = new SimpleDateFormat(this.dateFormat).format(calendar.getTime());
                path = this.filePath + "/" + timeStr;

            }else if("yesterday".equals(this.dateFunction)){
                calendar.set(Calendar.DAY_OF_YEAR, calendar.get(Calendar.DAY_OF_YEAR) - 1);
                timeStr = new SimpleDateFormat(this.dateFormat).format(calendar.getTime());
                path = this.filePath + "/" + timeStr;
            }else{
                path = this.filePath;
            }
            logger.info("go to check path {}", path);
            boolean isExist = new SftpUtil().isExist(this.user, this.password, this.host, this.port, path, pathSuffix, finalPaths);
            if(!isExist) {
                logger.warn("path " + path + " doesn't exist.");
            }else{
                if(finalPaths.size() > 0) {
                    logger.info("found files: {}", Arrays.toString(finalPaths.values().toArray()));
                    List<ChannelSftp.LsEntry> fileList = new ArrayList<ChannelSftp.LsEntry>(finalPaths.keySet());
                    Collections.sort(fileList, new Comparator<ChannelSftp.LsEntry>(){
                        @Override
                        public int compare(ChannelSftp.LsEntry file1, ChannelSftp.LsEntry file2) {
                            if(file1.getAttrs().getMTime() < file2.getAttrs().getMTime()){
                                return -1;
                            }else if(file1.getAttrs().getMTime()== file2.getAttrs().getMTime()){
                                return 0;
                            }else{
                                return 1;
                            }
                        }
                    });

                    int start = 0;
                    if (lastEmit > -1) {
                        start = lastEmit + 1;
                    }
                    int end = fileList.size(); //subList前包括后不包括
                    this.logger.info("TriggerSftpFlow execute, filePath = {}, fileList = {}, start = {}, end = {}", new Object[] { path, fileList, start, end});
                    if (end > start) {
                        List<ChannelSftp.LsEntry> toEmit = fileList.subList(start, end);
                        String[] arr = new String[toEmit.size()];
                        for (int i = 0; i < arr.length; i++) {
                            arr[i] = "sftp://"+this.host+":"+this.port + finalPaths.get(toEmit.get(i));
                        }
                        Map params = new HashMap();
                        params.put("eventStepInputPath", StringUtils.join(arr, ","));
                        this.logger.info("TriggerSftpFlow executing, filePath = {}, params = {}", path, params);
                        collector.emit(params, map);
                        lastEmit = end - 1;//lastEmit是保存的index
                        this.logger.info("TriggerSftpFlow execute done, filePath = {}, lastEmit = {}", this.filePath, lastEmit);
                    }
                }
            }

        } catch (Exception e) {
            logger.error("execute exception : ", e);
        }
    }

    public void cleanup()
    {
    }

}
