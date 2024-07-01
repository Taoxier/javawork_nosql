/*
 *@Type CmdClient.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 13:58
 * @version
 */
package client;
import java.util.Scanner;

public class CmdClient implements Client {
    private SocketClient socketClient;
    private Scanner scanner;

    public CmdClient(String host, int port) {
        this.socketClient = new SocketClient(host, port);
        this.scanner = new Scanner(System.in);
    }

    public void startInteractiveMode() {
        System.out.println("---【进入命令行客户端】---");
        boolean running = true;
        while (running) {
            System.out.print("--->请输入命令 ([set<k,v>]/[get<k>]/[rm<k>]/[exit]): ");
            String input = scanner.nextLine().trim();
            String[] parts = input.split(" ");
            if (parts.length < 1) {
                System.out.println("-->命令格式不正确，请检查后重试。");
                continue;
            }
            String action = parts[0];
            switch (action.toLowerCase()) {
                case "set":
                    if (parts.length != 3) {
                        System.out.println("-->请正确输入（set<key,value>）：");
                    } else {
                        socketClient.set(parts[1], parts[2]);
                    }
                    break;
                case "get":
                    if (parts.length != 2) {
                        System.out.println("-->请正确输入（get<key>）：");
                    } else {
                        socketClient.get(parts[1]);
                    }
                    break;
                case "rm":
                    if (parts.length != 2) {
                        System.out.println("-->请正确输入（rm<key>）：");
                    } else {
                        socketClient.rm(parts[1]);
                    }
                    break;
                case "exit":
                    running = false;
                    System.out.println("---【退出命令行客户端】---");
                    break;
                default:
                    System.out.println("--->未知命令，请输入 set/get/rm/exit 中的一个。");
            }
        }
        scanner.close();
    }

    @Override
    public void set(String key, String value) {

    }

    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public void rm(String key) {

    }
}