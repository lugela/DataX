package com.alibaba.datax.common.util;

import java.util.UUID;

public class KeyUtil {


    public static String genUniqueKey() {
        String uuid = UUID.randomUUID().toString().replaceAll("-","");
        return uuid;
    }
}
