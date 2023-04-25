import os
import logging
import time
from logging.handlers import RotatingFileHandler
from flask import Flask

logs_dir_path = os.path.realpath("logs")


# log配置，实现日志自动按日期生成日志文件
def make_dir(make_dir_path):
    path = make_dir_path.strip()
    if not os.path.exists(path):
        os.makedirs(path)


def get_log_handler():
    # 日志地址
    log_dir_name = "logs"
    # 文件名，以日期作为文件名
    log_file_name = 'logger-' + time.strftime('%Y-%m-%d', time.localtime(time.time())) + '.log'

    log_file_str = os.path.join(log_dir_name, log_file_name)

    # 默认日志等级的设置
    logging.basicConfig(level=logging.INFO)
    # 创建日志记录器，指明日志保存路径,每个日志的大小，保存日志的上限
    file_log_handler = RotatingFileHandler(log_file_str, maxBytes=1024 * 1024, backupCount=10, encoding='UTF-8')
    # 设置日志的格式                   发生时间    日志等级     日志信息文件名      函数名          行数        日志信息
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(lineno)s - %(message)s')
    # 将日志记录器指定日志的格式
    file_log_handler.setFormatter(formatter)

    # 日志等级的设置
    file_log_handler.setLevel(level=logging.INFO)

    return file_log_handler


class Logger:
    logs_dir_path = ""

    def __init__(self, logger_path="logs"):
        # 指定日志地址
        self.logs_dir_path = os.path.realpath(logger_path)

    def init_app(self, app: Flask) -> None:
        app.logger.addHandler(get_log_handler())

    def get_log_handler(self):
        # 文件名，以日期作为文件名
        log_file_name = 'logger-' + time.strftime('%Y-%m-%d', time.localtime(time.time())) + '.log'

        log_file_str = os.path.join(self.logs_dir_path, log_file_name)

        # 默认日志等级的设置
        logging.basicConfig(level=logging.INFO)
        # 创建日志记录器，指明日志保存路径,每个日志的大小，保存日志的上限
        file_log_handler = RotatingFileHandler(log_file_str, maxBytes=1024 * 1024, backupCount=10, encoding='UTF-8')
        # 设置日志的格式                   发生时间    日志等级     日志信息文件名      函数名          行数        日志信息
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(lineno)s - %(message)s')
        # 将日志记录器指定日志的格式
        file_log_handler.setFormatter(formatter)
        # 日志等级的设置
        file_log_handler.setLevel(level=logging.INFO)

        return file_log_handler
