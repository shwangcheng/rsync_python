# coding=utf-8

import os
import warnings
import subprocess
import contextlib


class Public(object):

    def __init__(self, port, timeout, lpath, ip, rpath, password_file, max_thread=5):

        # 进程控制：
        self.__mark = True
        self.__cmd_list = list()
        self.__proc_list = list()
        self.__max_thread = max_thread

        # 构建同步命令:
        self.port = port
        self.timeout = timeout
        self.lpath = lpath
        self.ip = ip
        self.rpath = rpath
        self.password_file = password_file

        self.sync = "rsync -rtpog --delete --port {port} --timeout={timeout} --inplace {lpath}/{spath} " \
                    "root@{ip}::{rpath} --password-file={password_file}"

    def __dirs(self):
        """
        获取 local 目录下的所有子目录
        :return: yield 子目录
        """
        for i in os.listdir(self.lpath):
            yield os.path.join(i)

    def __create_cmd(self):
        """
        生成 rsync 命令
        :return: yield rsync 命令
        """
        for spath in self.__dirs():
            cmd = self.sync.format(port=self.port,
                                   timeout=self.timeout,
                                   lpath=self.lpath,
                                   spath=spath,
                                   ip=self.ip,
                                   rpath=self.rpath,
                                   password_file=self.password_file)
            print cmd
            yield cmd

    def __run(self):
        """
        往列表中添加 rsync 命令， 但是始终控制在 __max_thread 以内
        :return:
        """
        for cmd in self.__create_cmd():
            if len(self.__cmd_list) < self.__max_thread:
                self.__cmd_list.append(cmd)
            else:
                break

    def start(self):
        """
        遍历命令列表，拿到 rsync 命令， 再使用 Popen 执行
        :return: 句柄列表， 可用来 kill
        """
        self.__run()
        for task_cmd in self.__cmd_list:
            self.__proc_list.append(subprocess.Popen(task_cmd, shell=True,
                                                     stdout=subprocess.PIPE,
                                                     stderr=subprocess.STDOUT))
        return self.__proc_list

    def kill(self):
        """
        配合抢占模式， 通过句柄列表 kill 掉所有 rsync 同步进程
        :return:
        """
        for i in self.__proc_list:
            i.kill()


if __name__ == '__main__':
    p = Public(22, 60, "/data", "192.168.3.251", "/data", "/rsyncd.passwd", 3)
    for proc in p.start():
        proc.wait()
        print("")







