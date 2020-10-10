# coding=utf-8

import os
import sys
import time
import logging
import paramiko
import threading


class Public(object):

    def __init__(self, max_thread):
        # 线程控制器:
        self.se = threading.Semaphore(max_thread)

    def async_call(self):
        """
        生成 rsync 命令
        :return: yield rsync 命令
        """
        yield None

    @staticmethod
    def cmd(cmd, se):
        """
        执行 rsync 命令, 通过 Semaphore 来控制线程数
        :param cmd: rsync 命令
        :param se: 线程控制器
        :return: None
        """
        s_time = time.time()
        se.acquire()
        try:
            os.system(cmd)
        except BaseException as e:
            logging.error(e)
        se.release()
        e_time = time.time()
        with open("/data/log", "a") as fil:
            fil.write("%s >> run time is：%.2f\n\n" % (cmd, e_time - s_time))

    def task_queue_consumer(self):
        """
        此函数读取 通过 async_call方法获取 rsync 命令, 通过线程再调用 cmd 方法,
        :return: 线程列表
        """
        thread_list = []
        for i in self.async_call():
            t = threading.Thread(target=self.cmd, args=(i, self.se))
            t.daemon = True
            t.start()
            thread_list.append(t)
        return thread_list


class FilesPush(Public):
    def __init__(self, l_abs_path, r_abs_path, r_user, r_addr, max_thread=5):
        super(FilesPush, self).__init__(max_thread)

        # 线程列表:
        self.thread_list = []

        # 操作目录:
        self.l_abs_path = l_abs_path
        self.r_abs_path = r_abs_path

        # 构建同步命令:
        self.r_user = r_user
        self.r_addr = r_addr
        self.sync = "rsync -avz " + "%s " + self.r_user + "@" + self.r_addr + ":" + "%s"

    def dirs(self):
        """
        获取 local 目录下的所有子目录
        :return: yield 子目录
        """
        for i in os.listdir(self.l_abs_path):
            yield os.path.join(i)

    def async_call(self):
        """
        生成 rsync 命令
        :return: yield rsync 命令
        """
        for i in self.dirs():
            cmd = self.sync % (self.l_abs_path + "/" + i, self.r_abs_path + "/")
            print "cmd: %s" % cmd
            yield cmd


class FilesPull(Public):
    def __init__(self, r_abs_path, l_abs_path, r_user, r_addr, r_password, r_port=22, max_thread=5):
        super(FilesPull, self).__init__(max_thread)

        # 实例化ssh客户端
        self.ssh = paramiko.SSHClient()

        # 创建默认的白名单
        self.policy = paramiko.AutoAddPolicy()

        # 设置白名单
        self.ssh.set_missing_host_key_policy(self.policy)

        # 链接服务器
        self.ssh.connect(hostname=r_addr,
                         port=r_port,
                         username=r_user,
                         password=r_password)

        # 线程列表:
        self.thread_list = []

        # 操作目录:
        self.r_abs_path = r_abs_path
        self.l_abs_path = l_abs_path

        # 构建同步命令:
        self.r_user = r_user
        self.r_addr = r_addr
        self.sync = "rsync -avz " + self.r_user + "@" + self.r_addr + ":" + "%s" + " %s"

    def dirs(self):
        """
        获取 remote 目录下的所有子目录
        :return: yield 子目录
        """
        stdin, stdout, stderr = self.ssh.exec_command("cd /data && ls")
        files = str(stdout.read().decode()).split("\n")
        for i in files:
            if i:
                yield i

    def async_call(self):
        """
        生成 rsync 命令
        :return: yield rsync 命令
        """
        for i in self.dirs():
            cmd = self.sync % (self.r_abs_path + "/" + i, self.l_abs_path + "/")
            print "cmd: %s" % cmd
            yield cmd


def p_try(pr, n):
    while True:
        if len(pr) >= 8:
            break
        pr.append(None)
    return pr[n]


if __name__ == '__main__':
    start_time = time.time()
    print "------------------------------------------------ start -----------------------------------------------"

    # 提取命令行参数:
    p = sys.argv
    print p

    if len(p) > 1:
        # push: ['test6.py', 'push', '/data', '/data', 'root', '192.168.3.201', '5']
        if p[1] == "push":
            files_push = FilesPush(l_abs_path=str(p_try(p, 2) or '/data'),
                                   r_abs_path=str(p_try(p, 3) or '/data'),
                                   r_user=str(p_try(p, 4) or 'root'),
                                   r_addr=str(p_try(p, 5) or '192.168.3.201'),
                                   max_thread=int(p_try(p, 6) if p_try(p, 6) else 10))
            t_list = files_push.task_queue_consumer()
            for info in t_list:
                info.join()

        # pull: ['test6.py', 'pull', '/data', '/data', 'root', '192.168.3.201', '123456', '5']
        elif p[1] == "pull":
            files_pull = FilesPull(r_abs_path=str(p_try(p, 2) or '/data'),
                                   l_abs_path=str(p_try(p, 2) or '/data'),
                                   r_user=str(p_try(p, 2) or 'root'),
                                   r_addr=str(p_try(p, 2) or '192.168.3.200'),
                                   r_password=str(p_try(p, 2) or '123456'),
                                   max_thread=int(p_try(p, 6) if p_try(p, 6) else 10))
            t_list = files_pull.task_queue_consumer()
            for info in t_list:
                info.join()

    else:
        print """
        ---------------------------------------------------------------
        push 操作参数:
              示例: python XXX.py push /data1 /data2 root 192.168.3.201 5
              解释:  push          : 操作方式(将本地目录/文件推送到远程备份服务器)
                    /data1        : 本地路径
                    /data2        : 远程路径
                    root          : 远程主机用户名
                    192.168.3.201 : 远程主机IP
                    5             : 线程数
        ---------------------------------------------------------------
        pull 操作参数:
              示例: python XXX.py pull /data1 /data2 root 192.168.3.201 123456 5
              解释:  push          : 操作方式(将本地目录/文件推送到远程备份服务器)
                    /data1        : 本地路径
                    /data2        : 远程路径
                    root          : 远程主机用户名
                    192.168.3.201 : 远程主机IP
                    123456        : 远程主机密码
                    5             : 线程数
        ---------------------------------------------------------------
              """

    print "------------------------------------------------ end -------------------------------------------------"
    print '%d second' % (time.time() - start_time)
