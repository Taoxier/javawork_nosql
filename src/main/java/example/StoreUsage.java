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
        NormalStore store = new NormalStore(dataDir,2,1,3);
//        store.set("zsy1","1");
//        store.set("zsy2","2");
//        store.set("zsy3","3");
        store.set("test1","test11");
        store.set("tests2","test22");
        System.out.println(store.get("test1"));//你好
        store.rm("test1");
        System.out.println(store.get("test1"));//null

        store.set("test3","aaaaaaaa");
        store.set("test3","bbbbbb");
        store.set("test3","cccccc");
        store.set("test3","dddddddddd");
        store.set("test3","eeeeee");
        store.set("test3","ffffffff");
//        store.set("wc","nd");
        System.out.println(store.get("test3"));
//        store.rm("test2");

//        store.printSsTablesFileName();
//        store.rm("haha");
//        System.out.println(store.get("haha"));
//        System.out.println(store.get("wc"));


    }
}
