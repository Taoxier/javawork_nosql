package service;

import java.io.File;
import java.io.IOException;

/**
 * @Author taoxier
 * @Date 2024/7/31 2:30
 * @描述
 */
public class test {
    public static void main(String[] args) throws IOException {
        File file=new File("data/test");
        if (!file.exists()){
            file.createNewFile();
            System.out.println("创建文件");
        }
        if (file.delete()){
            System.out.println("shanchu");
        }
    }
}
