package com.inforefiner.eventgateway.utils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

/**
 * @Author: haijun
 * @Date: 2020/6/10 9:57
 */
public class SftpUtil {
    static private final Logger log = LoggerFactory.getLogger(SftpUtil.class);

    static private Session session = null;
    static private Channel channel = null;
    static private int timeout = 60000; //超时数,一分钟

    public static ChannelSftp getChannel(String username, String password, String host, String port) throws JSchException {
        JSch jsch = new JSch(); // 创建JSch对象
        // 根据用户名，主机ip，端口获取一个Session对象
        session = jsch.getSession(username, host, Integer.valueOf(port));
        log.info("Session created...");
        if (password != null) {
            session.setPassword(password); // 设置密码
        }
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config); // 为Session对象设置properties
        session.setTimeout(timeout); // 设置timeout时间
        session.connect(); // 通过Session建立链接
        log.info("Session connected, Opening Channel...");
        channel = session.openChannel("sftp"); // 打开SFTP通道
        channel.connect(); // 建立SFTP通道的连接
        log.info("Connected successfully to ip :{}, ftpUsername is :{}, return :{}",
                host,username, channel);
        return (ChannelSftp) channel;
    }

    /**
     * 关闭channel和session
     * @throws Exception
     */
    public static void closeChannel() throws Exception {
        if (channel != null) {
            channel.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }

    public static boolean isExist(String username,String password,String host,String port, String path, String pathSuffix, Map<ChannelSftp.LsEntry,String> finalEntrys){
        ChannelSftp channelSftp = null;
        try {
            // 一、 获取channelSftp对象
            channelSftp = getChannel(username, password, host, port);
            // 二、 判断远程路径dstDirPath是否存在(通道配置的路径)
            boolean isDir = false;
            String oldPath = path;
            try {
                channelSftp.cd(path);
                isDir = true;
            }catch (Exception e){
                isDir = false;
            }
            try {
                Vector dir = channelSftp.ls(path);
                if (dir == null || dir.isEmpty()) {
                    return false;
                }else{
                    if(isDir){
                        if(!oldPath.endsWith("/")){
                            oldPath = oldPath + "/";
                        }
                        Enumeration elements = dir.elements();
                        while(elements.hasMoreElements()){
                            ChannelSftp.LsEntry lsEntry = (ChannelSftp.LsEntry)elements.nextElement();
                            if(!lsEntry.getFilename().equals(".") && !lsEntry.getFilename().equals("..") && lsEntry.getFilename().endsWith(pathSuffix)) {
                                finalEntrys.put(lsEntry, oldPath + lsEntry.getFilename());
                            }
                        }
                    }else{
                        ChannelSftp.LsEntry file = (ChannelSftp.LsEntry)dir.firstElement();
                        finalEntrys.put(file, oldPath);//具体文件不用pathSuffix限制
                    }

                    return true;
                }
            } catch (SftpException e) { // 如果dstDirPath不存在，则会报错，此时捕获异常并创建dstDirPath路径
                log.error("dstPath {} doesn't exist", path);
                return false;
            }

        }catch (Exception e){
            throw new RuntimeException("create channel exception: ", e);
        }  finally {
            // 处理后事
            if (channelSftp != null)
                channelSftp.quit();
            try {
                closeChannel();
            } catch (Exception e) {
                log.error("closeChannel exception",e);
            }
        }
    }

    public static boolean checkSpecialed(String username,String password,String host,String port, String file){
        ChannelSftp channelSftp = null;
        try {
            // 一、 获取channelSftp对象
            channelSftp = getChannel(username, password, host, port);
            // 二、 判断远程路径dstDirPath是否存在(通道配置的路径)
            try {
                Vector dir = channelSftp.ls(file);
                if (dir == null || dir.isEmpty()) {
                    return false;
                }else{
                    ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry)dir.firstElement();
                    if(System.currentTimeMillis()/1000 - entry.getAttrs().getMTime() > 60) {//修改时间超过了一分钟
                        //rename
                        channelSftp.rename(file, file+ ".bak");

                        return true;
                    }else{
                        return false;
                    }
                }
            } catch (SftpException e) { // 如果dstDirPath不存在，则会报错，此时捕获异常并创建dstDirPath路径
                log.error("dstPath {} doesn't exist", file);
                return false;
            }
        }catch (Exception e){
            throw new RuntimeException("create channel exception: ", e);
        }  finally {
            // 处理后事
            if (channelSftp != null)
                channelSftp.quit();
            try {
                closeChannel();
            } catch (Exception e) {
                log.error("closeChannel exception",e);
            }
        }
    }

    public static void main(String[] args){
        Map<ChannelSftp.LsEntry,String> finalPaths = new HashMap<ChannelSftp.LsEntry,String>();
        isExist("merce", "merce", "info2", "22", "/home/merce/20200521", ".csv", finalPaths);
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
        int lastEmit = -1;
        if (lastEmit > -1) {
            start = lastEmit + 1;
        }
        int end = fileList.size(); // keep last one
        if (end > start) {
            List<ChannelSftp.LsEntry> toEmit = fileList.subList(start, end);
            lastEmit = end - 1;
        }
    }
}
