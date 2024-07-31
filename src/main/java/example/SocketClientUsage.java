/*
 *@Type SocketClientUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:07
 * @version
 */
package example;

import client.Client;
import client.SocketClient;

public class SocketClientUsage {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 12345;
        Client client = new SocketClient(host, port);

//        client.set("haha","aaaaaaaa");
//        client.set("wc","nd");
//        client.get("haha");
//        client.rm("haha");
//        client.get("haha");
//        client.get("wc");
//
//        client.set("abcde","hihihihihihihhi");
//        client.get("abcde");
//        client.set("what","nani");
//        client.set("what","nihaonihao");
//        client.get("what");

        client.set("test11","11test");
        client.get("test11");
        client.rm("test11");
        client.get("test11");


    }
}