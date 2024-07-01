package example;

import client.CmdClient;

/**
 * @Author taoxier
 * @Date 2024/7/2 2:52
 * @注释
 */
public class CmdClientUsage {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("---【使用方法：Main <server_host> <server_port>】---");
            return;
        }

        String serverHost = args[0];
        int serverPort;
        try {
            serverPort = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.err.println("--->端口号必须为数字");
            return;
        }

        // 使用命令行参数实例化CmdClient
        CmdClient cmdClient = new CmdClient(serverHost, serverPort);

        // 启动交互模式
        cmdClient.startInteractiveMode();
    }
}
