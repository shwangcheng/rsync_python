# coding=utf-8
import os
import sys
import time
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

        # 进程信息：
        self.gt_list = list()
        self.proc_list = list()
        self.GREEN_POOL = eventlet.GreenPool(max_thread)

        # 日志模块：
        self.LOG = get_logger(__name__)

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
        ret_code, ret_info = -1, ""
        proc = subprocess.Popen(task_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pid = proc.pid
        self.proc_list.append(proc)
        self.LOG.debug("RsyncStart: "
                       "pid：{pid} "
                       "task_cmd: {task_cmd}".format(pid=pid, task_cmd=task_cmd))
        try:
            ret_code = proc.wait(1500)
            ret_info = proc.stdout.read()
        except subprocess.TimeoutExpired as error:
            self.LOG.debug("TimeoutExpired: "
                           "pid：{pid} "
                           "ret_code：{ret_code} "
                           "ret_info：{ret_info}\n".format(pid=pid, ret_code=ret_code, ret_info=ret_info))
            proc.kill()
            proc.wait()

        except Exception as error:
            self.LOG.exception(error)
            proc.kill()
            proc.wait()

    def kill(self):
        """
        杀死所有进程，包括协程。
        :return:
        """
        for i in self.proc_list:
            try:
                i.kill()
            except OSError:
                pass
        del self.proc_list

        for i in self.gt_list:
            eventlet.kill(i)

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

    time.sleep(5)

    subproc.kill()

    subproc.GREEN_POOL.waitall()

