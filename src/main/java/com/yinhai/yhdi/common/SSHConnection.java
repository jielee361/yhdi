package com.yinhai.yhdi.common;

import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.IOptionName;
import net.neoremind.sshxcute.core.Result;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.task.CustomTask;
import net.neoremind.sshxcute.task.impl.ExecCommand;

public class SSHConnection {
    /**
     * 获取ssh通用方法
     *
     * @param hostip
     * @param username
     * @param password
     * @return
     */
    public static SSHExec getConnection(String hostip, String username, String password) {
        ConnBean conn = new ConnBean(hostip, username, password);
        SSHExec ssh = new SSHExec(conn);
        return ssh;
    }

    /**
     * 连接ssh,执行Linux命令
     *
     * @param url      主机地址
     * @param username 用户名
     * @param password 密码
     * @param cmd      linux命令(多个命令用','隔开)
     * @return 返回执行结果
     * @throws Exception
     */

    public static Result excuteCmd(String url, String username, String password, String cmd) throws Exception {

        SSHExec ssh = getConnection(url, username, password);
        SSHExec.setOption(IOptionName.INTEVAL_TIME_BETWEEN_TASKS, 2000l);
        try {
            if (ssh.connect()) {
                CustomTask task = new ExecCommand(cmd);
                Result result = ssh.exec(task);
                ssh.disconnect();
                return result;
            } else {
                ssh.disconnect();
                throw new Exception("连接主机失败");
            }
        } catch (Exception e) {
            ssh.disconnect();
            throw e;
        }
    }
}
