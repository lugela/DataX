package com.alibaba.datax.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class ShellUtil {
    private static final Logger logger = LoggerFactory
            .getLogger(ShellUtil.class);
    public static boolean exec(String[] cmd){
        boolean flag = false;
        BufferedReader bufferedReader = null;
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("准备执行Shell命令 ").append(cmd)
                .append(" \r\n");
        logger.info("准备执行Shell命令 "+cmd);
        try {
            Process pid =  Runtime.getRuntime().exec(cmd);
            if (pid != null) {
                stringBuffer.append("进程号：").append(pid.toString())
                        .append("\r\n");
                logger.info("进程号 "+pid.toString());

                // bufferedReader用于读取Shell的输出内容
                bufferedReader = new BufferedReader(new InputStreamReader(pid.getInputStream()), 1024);
                pid.waitFor();
            } else {
                stringBuffer.append("没有pid\r\n");
                logger.info("没有pid");

            }

            stringBuffer.append(
                    "Shell命令执行完毕\r\n执行结果为：\r\n");
            String line = null;
            // 读取Shell的输出内容，并添加到stringBuffer中
            while (bufferedReader != null
                    && (line = bufferedReader.readLine()) != null) {
                stringBuffer.append(line).append("\r\n");
            }
            logger.info(stringBuffer.toString());
            flag = true;
        } catch (Exception e) {
            logger.error(e.getMessage());
        }finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
        logger.info("是否成功"+flag);

        return flag;
    }
}
