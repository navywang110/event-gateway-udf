package com.inforefiner.eventgateway;

import com.inforefiner.eventgateway.utils.SftpUtil;
import com.jcraft.jsch.ChannelSftp;
import com.merce.woven.common.workflow.sdk.ProcessDelegate;
import com.merce.woven.common.workflow.sdk.ProcessOutCollector;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TriggerByChecked
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
    private String flagFile;//checked complete文件名称

    public TriggerByChecked()
    {
        this.logger = LoggerFactory.getLogger(TriggerByChecked.class);
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
        this.flagFile = props.getProperty("flagFile");
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


            boolean specialed = SftpUtil.checkSpecialed(this.user, this.password, this.host, this.port, path + "/" + this.flagFile);

            if(specialed){
                boolean isExist = SftpUtil.isExist(this.user, this.password, this.host, this.port, path, pathSuffix, finalPaths);
                if(!isExist) {
                    logger.warn("path " + path + " doesn't exist.");
                }else{
                    if(finalPaths.size() > 0) {
                        Object[] toEmit = finalPaths.values().toArray();
                        logger.info("found files: {}", Arrays.toString(finalPaths.values().toArray()));
                        logger.info("TriggerByChecked execute, filePath = {}, fileList = {}, start = {}, end = {}", new Object[] { path, finalPaths.values().toArray(), 0, finalPaths.size()});
                        String[] arr = new String[toEmit.length];
                        for (int i = 0; i < arr.length; i++) {
                            arr[i] = "sftp://"+this.host+":"+this.port + (String)toEmit[i];
                        }
                        Map params = new HashMap();
                        params.put("eventStepInputPath", StringUtils.join(arr, ","));
                        this.logger.info("TriggerByChecked executing, filePath = {}, params = {}", path, params);
                        collector.emit(params, map);
                        this.logger.info("TriggerByChecked execute done, filePath = {}, totalLength = {}", this.filePath, arr.length);
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
