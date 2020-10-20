# coding=utf-8
import os
import sys
import time
import signal
import logging
import eventlet
from eventlet.green import subprocess

eventlet.monkey_patch()

__LOGFILE_NAME__ = "%s.log" % os.path.basename(sys.argv[0])


def get_logger(name):
    formatter = logging.Formatter('%(levelname)s: %(asctime)s %(funcName)s(%(lineno)d) -- %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(name)
    log_path = os.path.join(__LOGFILE_NAME__)
    fh = logging.FileHandler(log_path)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(logging.DEBUG)
    return logger


class SubProc:
    def __init__(self, port, timeout, lpath, ip, rpath, password_file, max_thread=5):

        # 协程列表：
        self.gt_list = list()
        # 进程信息：
        self.ret_code = 0
        self.ret_info = "Null"
        # 日志模块：
        self.LOG = get_logger(__name__)

        # 进程控制：
        self.proc_list = list()
        self.GREEN_POOL = eventlet.GreenPool(max_thread)

        # 构建同步命令:
        self.port = port  # 远程端口
        self.timeout = timeout  # 超时时间
        self.lpath = lpath  # 本地目录
        self.ip = ip  # 远程IP
        self.rpath = rpath  # 远程目录
        self.password_file = password_file  # 密码文件路径

        # rsync 同步命令：
        self.sync = "rsync -rtpog --delete --port {port} --timeout={timeout} --inplace {lpath}/{spath} " \
                    "root@{ip}::{rpath} --password-file={password_file}"

    def dirs(self):
        """
        获取 local 目录下的所有子目录
        :return: yield 子目录
        """
        for i in os.listdir(self.lpath):
            yield os.path.join(i)

    def create_cmd(self):
        """
        生成 rsync 命令
        :return: yield rsync 命令
        """
        for spath in self.dirs():
            cmd = self.sync.format(port=self.port,
                                   timeout=self.timeout,
                                   lpath=self.lpath,
                                   spath=spath,
                                   ip=self.ip,
                                   rpath=self.rpath,
                                   password_file=self.password_file)
            yield cmd

    def start(self, task_cmd):
        """
        rsync 同步执行
        :param task_cmd: rsync 命令
        :return:
        """
        try:
            proc = subprocess.Popen(task_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            self.proc_list.append(proc)
            self.LOG.debug("task_cmd: {task_cmd}".format(task_cmd=task_cmd))
            self.ret_code = proc.wait(100)
            self.ret_info = proc.stdout.read()
            self.LOG.debug("ret_info：{ret_info}ret_code：{ret_code}\n"
                           .format(ret_info=self.ret_info, ret_code=self.ret_code))
        except BaseException as error:
            self.LOG.debug("error: {error}\nret_info：{ret_info}ret_code：{ret_code} \ntask_cmd: {task_cmd}\n"
                           .format(error=error, ret_info=self.ret_info, ret_code=self.ret_code, task_cmd=task_cmd))

    def kill(self):
        """
        调用此方法会将所有同步中的进程杀死， 配合抢占模式
        :return:
        """
        for gt in self.gt_list:
            """kill 掉所有的协程！"""
            gt.kill()

        for proc in self.proc_list:
            """kill 掉所有的同步进程！"""
            os.killpg(proc.pid, signal.SIGUSR1)

        exit(0)

    def main(self):
        """
        使用协程启动同步进程，控制并发量
        :return: 协程列表
        """
        for task_cmd in self.create_cmd():
            self.gt_list.append(self.GREEN_POOL.spawn(self.start, task_cmd))


if __name__ == '__main__':
    subproc = SubProc(port=873, timeout=60, lpath="/mount_path", ip="192.168.3.251",
                      rpath="mount_path", password_file="/etc/rsyncd.passwd", max_thread=5)
    subproc.main()
    subproc.GREEN_POOL.waitall()

