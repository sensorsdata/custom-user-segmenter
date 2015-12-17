# -*- coding: UTF-8 -*-

"""
Copyright (c) 2015 Sensors Data, Inc. All Rights Reserved
@author cfreely(fulili@baidu.com)
@brief

实现自定义用户分群的辅助工具
"""
import sys

# Sensors Analytics Python SDK 地址，如果不正确请自行修改
sys.path.append('/home/sa_standalone/sa/sdk/python/')

if sys.version_info[0] != 3:
    raise Exception("Please use Python3 for this tool")

import argparse
import logging
import traceback
import urllib.request
import urllib.parse
import sensorsanalytics

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout,
                    format='%(asctime)s %(lineno)d %(levelname)s %(message)s')


class CustomUserSegmenter:
    def __init__(self, api_url, logging_url, api_token, temp_dir, sql_file, profile_name):
        self._api_url = api_url
        self._logging_url = logging_url
        self._api_token = api_token
        self._temp_dir = temp_dir
        self._sql_file = sql_file
        self._profile_name = profile_name

    def _execute_sql(self, sql):
        params = urllib.parse.urlencode({'q': sql.encode('utf8'), 'token': self._api_token})
        try:
            res = urllib.request.urlopen("%s/sql/query?%s" % (self._api_url, params))
            return 200, res
        except urllib.request.HTTPError as e:
            logging.error(traceback.format_exc())
            return e.code, e.read()

    def run(self):
        # 从文件里读取 SQL
        try:
            fh = open(self._sql_file, 'r')
            sql = fh.read()
            fh.close()
        except IOError:
            logging.error(traceback.format_exc())
            logging.error("fail to read sql from file")
            return False

        # 获取 Meta 信息，判断是否是第一次导入
        meta_sql = 'SELECT * FROM users LIMIT 1'
        ret_code, result = self._execute_sql(meta_sql)
        if ret_code != 200:
            logging.error("fail to execute meta sql. [msg=%r]", result.decode('utf8'))
            return False
        columns = result.readline().decode('utf8').strip().split('\t')
        is_new_profile = True
        if self._profile_name.upper() in (column.upper() for column in columns):
            is_new_profile = False
        result.close()

        # 构造 SQL，需要和 users 表进行 JOIN
        if is_new_profile:
            filter_sql = 'b.segment IS NOT NULL'
        else:
            filter_sql = 'a.%s IS NOT NULL OR b.segment IS NOT NULL ' % self._profile_name

        final_sql = '''
            SELECT
                DISTINCT COALESCE(b.distinct_id, a.first_id), b.segment
            FROM users a
            LEFT OUTER JOIN (%s) b ON a.id = b.user_id
            WHERE %s
            LIMIT 100000000
         ''' % (sql, filter_sql)
        logging.info("start running segmenter sql. [sql=%r, first=%r]", sql.strip(), is_new_profile)

        # 执行 SQL
        ret_code, result = self._execute_sql(final_sql)
        if ret_code != 200:
            logging.error("fail to execute segmenter sql. [msg=%r]", result.decode('utf8'))
            return False

        # 打开临时文件待写入
        tmp_data_file_path = self._temp_dir + '/segmenter.dat'
        try:
            tmp_data_file_handler = open(tmp_data_file_path, 'w+')
        except IOError:
            logging.error(traceback.format_exc())
            logging.error("fail to open tmp file for write")
            return False

        # 写入临时文件，因为后面的发送可能需要比较长的时间
        logging.info("write segmenter result to tmp file. [path=%r]", tmp_data_file_path)
        first = True
        row_num = 0
        for record in result:
            if first:
                first = False
                continue
            tmp_data_file_handler.write(record.decode('utf8'))
            row_num += 1
        result.close()
        logging.info("write segmenter result success. [size=%r]", row_num)

        # 使用 SDK 开始发送数据
        logging.info("start sending data to Sensors Analytics.")
        consumer = sensorsanalytics.BatchConsumer(self._logging_url, max_size=100)
        sa = sensorsanalytics.SensorsAnalytics(consumer)
        tmp_data_file_handler.seek(0, 0)
        for record in tmp_data_file_handler:
            distinct_id, segment = record.split('\t')
            if len(distinct_id) == 0:
                continue
            segment = segment.strip()
            if len(segment) == 0:
                sa.profile_unset(distinct_id, [self._profile_name])
            else:
                sa.profile_set(distinct_id, {self._profile_name: segment})
        sa.close()
        tmp_data_file_handler.close()
        logging.info("send data success.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--api_url", help="Sensors Analytics api url", required=True)
    parser.add_argument("-t", "--api_token", help="Sensors Analytics api token", required=True)
    parser.add_argument("-l", "--logging_url", help="Sensors Analytics logging url", required=True)
    parser.add_argument("-d", "--temp_dir", help="temporary directory", required=True)
    parser.add_argument("-f", "--sql_file", help="segmenter SQL in a file", required=True)
    parser.add_argument("-n", "--profile_name", help="segmenter profile name", required=True)
    args = parser.parse_args()
    segmenter = CustomUserSegmenter(args.api_url, args.logging_url, args.api_token,
                                    args.temp_dir, args.sql_file, args.profile_name)
    segmenter.run()
