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
//        client.get("zsy1");
//        client.set("zsy12","for test");
//        client.get("zsy12");
//        client.rm("zsy12");
//        client.get("zsy12");

        client.set("haha","aaaaaaaa");
        client.set("wc","nd");
        client.get("haha");
        client.rm("haha");
        client.get("haha");
        client.get("wc");

        client.set("abcde","hihihihihihihhi");
        client.get("abcde");
        client.set("what","nani");
        client.set("what","nihaonihao");
        client.get("what");

    }
}