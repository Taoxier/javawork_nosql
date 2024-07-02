/*
 *@Type Usage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 03:59
 * @version
 */
package example;

import service.NormalStore;

import java.io.File;

public class StoreUsage {
    public static void main(String[] args) {
        String dataDir="data"+ File.separator;
        NormalStore store = new NormalStore(dataDir);
//        store.set("zsy1","1");
//        store.set("zsy2","2");
//        store.set("zsy3","3");
        store.set("zsy4","你好");
        store.set("wc","nd");
        System.out.println(store.get("zsy4"));
        store.rm("zsy4");
        System.out.println(store.get("zsy4"));

        store.set("haha","aaaaaaaa");
        store.set("haha","bbbbbb");
        store.set("haha","cccccc");
        store.set("haha","dddddddddd");
        store.set("haha","eeeeee");
        store.set("haha","ffffffff");
//        store.set("wc","nd");
        System.out.println(store.get("haha"));
        store.rm("wc");
//        store.rm("haha");
//        System.out.println(store.get("haha"));
//        System.out.println(store.get("wc"));



    }
}
