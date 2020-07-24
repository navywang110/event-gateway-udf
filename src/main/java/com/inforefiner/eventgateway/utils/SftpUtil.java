package com.inforefiner.eventgateway.utils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
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

    private Session session = null;
    private Channel channel = null;
    private int timeout = 60000; //超时数,一分钟

    public ChannelSftp getChannel(String username, String password, String host, String port) throws JSchException {
        JSch jsch = new JSch(); // 创建JSch对象
        // 根据用户名，主机ip，端口获取一个Session对象
        session = jsch.getSession(username, host, Integer.valueOf(port));
        log.info("Session created ,go to connect...");
        if (password != null) {
            session.setPassword(password); // 设置密码
        }
        Properties config = new Properties();
        config.put("kex", "diffie-hellman-group1-sha1,diffie-hellman-group14-sha1,diffie-hellman-group-exchange-sha1,diffie-hellman-group-exchange-sha256");
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config); // 为Session对象设置properties
        session.setTimeout(timeout); // 设置timeout时间
        session.connect(); // 通过Session建立链接
        log.info("Session connected, Opening Channel...");
        channel = session.openChannel("sftp"); // 打开SFTP通道
        channel.connect(); // 建立SFTP通道的连接
        log.info("Connected successfully to ip :{}, ftpUsername is :{}, return :{}",
                host,username, channel);
        System.out.println("connect suceess ....");
        return (ChannelSftp) channel;
    }

    /**
     * 关闭channel和session
     * @throws Exception
     */
    public void closeChannel() throws Exception {
        if (channel != null) {
            channel.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }

    public boolean isExist(String username,String password,String host,String port, String path, String pathSuffix, String bussinessPrefix, Map<ChannelSftp.LsEntry,String> finalEntrys){
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
                        if(bussinessPrefix == null)
                            bussinessPrefix = "";
                        Enumeration elements = dir.elements();
                        while(elements.hasMoreElements()){
                            ChannelSftp.LsEntry lsEntry = (ChannelSftp.LsEntry)elements.nextElement();
                            if(!lsEntry.getFilename().equals(".") && !lsEntry.getFilename().equals("..") && lsEntry.getFilename().startsWith(bussinessPrefix) && lsEntry.getFilename().endsWith(pathSuffix)) {
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
                log.error("isExist dstPath "+path+" doesn't exist", e);
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

    /**
     *
     * @param username
     * @param password
     * @param host
     * @param port
     * @param file
     * @return check文件全路径
     */
    public String checkSpecialed(String username,String password,String host,String port, String file){
        ChannelSftp channelSftp = null;
        try {
            // 一、 获取channelSftp对象
            channelSftp = getChannel(username, password, host, port);
            // 二、 判断远程路径dstDirPath是否存在(通道配置的路径)
            try {
                Vector dir = channelSftp.ls(file);
                if (dir == null || dir.isEmpty()) {
                    return null;
                }else{
                    ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry)dir.firstElement();
                    if(System.currentTimeMillis()/1000 - entry.getAttrs().getMTime() > 60) {//修改时间超过了一分钟
                        //用rename保证原子性操作,保证多线程安全性
                        String parDir = file.substring(0, file.lastIndexOf("/"));
                        channelSftp.rename(parDir + "/" + entry.getFilename(), parDir + "/" + entry.getFilename() + ".bak");
                        return parDir + "/" + entry.getFilename();
                    }else{
                        return null;
                    }
                }
            } catch (SftpException e) { // 如果dstDirPath不存在，则会报错，此时捕获异常并创建dstDirPath路径
                log.error("checkSpecialed dstPath "+file+" doesn't exist : " + e.getMessage());
                return null;
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

    public boolean rename(String username,String password,String host,String port, String srcFile,String dstFile){
        ChannelSftp channelSftp = null;
        try {
            // 一、 获取channelSftp对象
            channelSftp = getChannel(username, password, host, port);
            // 二、 判断远程路径dstDirPath是否存在(通道配置的路径)
            try {
                Vector dir = channelSftp.ls(srcFile);
                if (dir == null || dir.isEmpty()) {
                    return false;
                }else{
                    channelSftp.rename(srcFile, dstFile);
                    return true;
                }
            } catch (SftpException e) { // 如果dstDirPath不存在，则会报错，此时捕获异常并创建dstDirPath路径
                log.error("rename " + srcFile+ " error : ", e);
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
        SftpUtil sftp = new SftpUtil();
        Map<ChannelSftp.LsEntry,String> finalPaths = new HashMap<ChannelSftp.LsEntry,String>();
        try{
            sftp.checkSpecialed("root", "123456", "192.168.1.188", "22","/root/ftp_617/2020-06-14/*.chk");
        }catch (Exception e){
            e.printStackTrace();
        }

        System.out.println("=====================分割线================");

        try{
            sftp.checkSpecialed("root", "merce@inforefiner", "192.168.1.189", "22","/root/ftp_617/2020-06-14/*.chk");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
