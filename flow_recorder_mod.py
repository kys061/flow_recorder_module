
#####################################
# Copyright (c) 2017 Saise          #
# Last Date : 2017.07.21            #
# Writer : yskang(kys061@gmail.com) #
#####################################
'''
    flow_recorder_module
    ~~~~~~~~~~~~~~~~~~~

    flow_recorder_module is a support tool.
    It provides a below module.

    1. common module
    ----------------
        :For monitor and recorder

    2. monitor module
    -----------------
        :For monitor only

    3. recorder module
    ------------------
        :For recorder only

'''

import os
import subprocess
from datetime import datetime, timedelta
import shutil
import re
import csv
import logging
import tarfile
import pandas as pd
###############################################################################
# User Setting
###############################################################################
USERNAME = 'admin'
PASSWORD = 'admin'

# percentage of usage disk(/) of df command
LIMIT_DISK_SIZE = 55
# 1000 = 1Kbyte, 1000000 = 1Mbyte, 50000000 = 50Mbyte
LOGSIZE = 50000000
###############################################################################

# check if archive is done or not.
archive_count = 1
# Init path and filename
FLOW_LOG_FOLDER_PATH = r'/var/log/flows'
FLOW_USER_LOG_FOLDER = r'/var/log/flows/users'
SCRIPT_MON_LOG_FILE = r'/var/log/flow_recorder.log'
SCRIPT_MON_LOG_FOLDER = r'/var/log/'
STM_SCRIPT_PATH = r'/opt/stm/target/pcli/stm_cli.py'

# for monitor script
SCRIPT_PATH = r'/etc/stmfiles/files/scripts/'
SCRIPT_FILENAME = r'flow_recorder.py'
MON_LOG_FILENAME = r'flow_recorder.log'
RECORDER_SCRIPT_FILENAME = r'flow_recorder.py'
MONITOR_SCRIPT_FILENAME = r'flow_recorder_monitor.py'
# recorder logger setting
logger_recorder = logging.getLogger('saisei.flow.recorder')
logger_recorder.setLevel(logging.INFO)
logger_monitor = logging.getLogger('saisei.flow.recorder.monitor')
logger_monitor.setLevel(logging.INFO)
logger_common = logging.getLogger('saisei.flow.recorder.common')
logger_common.setLevel(logging.INFO)

handler = logging.FileHandler(SCRIPT_MON_LOG_FILE)
handler.setLevel(logging.INFO)
filter = logging.Filter('saisei.flow')
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
handler.addFilter(filter)

logger_recorder.addHandler(handler)
logger_recorder.addFilter(filter)
logger_monitor.addHandler(handler)
logger_monitor.addFilter(filter)
logger_common.addHandler(handler)
logger_common.addFilter(filter)

# pattern for re
pattern_for_top = 'top [0-9]+'

pattern = r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
#pattern_01 = r'[-]{2,10}'
pattern_01 = r'[^a-zA-Z][^0-9][-][^a-zA-Z][^0-9]+'
pattern_03 = r' +\n'
#pattern_04 = r'Flows at [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
pattern_04 = r'Flows at [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\n\s+'
#pattern_05 = r'[,\s+]{23}'
pattern_06 = r'Flows at'
pattern_07 = r'Flows at '
#
is_extracted = False
#
err_lists = ['Cannot connect to server', 'does not exist', 'no matching objects',
             'waiting for server', 'cannot connect to server']
#
get_interface_cmd = 'echo \'show interfaces\' | sudo /opt/stm/target/pcli/stm_cli.py '+USERNAME+':'+PASSWORD+'@localhost |egrep \'[Internal|External]\' |grep Ethernet |awk \'{print $1}\''
################################################################################
#                       Common Module
################################################################################
# Excute command in shell
def subprocess_open(command):
    try:
        popen = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (stdoutdata, stderrdata) = popen.communicate()
    except Exception as e:
        logger_common.error("subprocess_open() cannot be executed, {}".format(e))
        pass
    return stdoutdata, stderrdata

# Parse the current date
def parsedate(today_date):
    try:
        parseDate = today_date.split(':')
        year = parseDate[0]
        month = parseDate[1]
        day = parseDate[2]
    except Exception as e:
        logger_common.error("parsedate() cannot be executed, {}".format(e))
        pass
    return [year, month, day]

# Get current date as LIST(y:m:d, y/m/d h:m:s).
def get_nowdate():
    try:
        nowdate = datetime.today().strftime("%Y:%m:%d")
        nowdatetime = datetime.today().strftime("%Y/%m/%d %H:%M:%S")
    except Exception as e:
        logger_common.error("get_nowdate() cannot be executed, {}".format(e))
        pass
    return [nowdate, nowdatetime]
################################################################################
#                      Flow.Monitor  Class with generator
################################################################################
class GetFilenames(object):
    def __init__(self, dirpath):
        self._dirpath = dirpath
    def __iter__(self):
        filelist = os.listdir(self._dirpath)
        for filename in filelist:
            yield filename

class GetRow(object):
    def __init__(self, result):
        self._result = result
    def __iter__(self):
        for row in self._result:
            yield row
################################################################################
#                      Flow.Monitor  Module
################################################################################
def init_logger():
    # recorder logger setting
    global logger_recorder, logger_monitor, logger_common
    global handler, filter, formatter

    for hdlr in logger_recorder.handlers[:]: # remove all old handlers
        logger_recorder.removeHandler(hdlr)
    for hdlr in logger_monitor.handlers[:]: # remove all old handlers
        logger_monitor.removeHandler(hdlr)
    for hdlr in logger_common.handlers[:]: # remove all old handlers
        logger_common.removeHandler(hdlr)

    logger_recorder = logging.getLogger('saisei.flow.recorder')
    logger_recorder.setLevel(logging.INFO)
    logger_monitor = logging.getLogger('saisei.flow.recorder.monitor')
    logger_monitor.setLevel(logging.INFO)
    logger_common = logging.getLogger('saisei.flow.recorder.common')
    logger_common.setLevel(logging.INFO)

    handler = logging.FileHandler(SCRIPT_MON_LOG_FILE)
    handler.setLevel(logging.INFO)
    filter = logging.Filter('saisei.flow')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    handler.addFilter(filter)

    logger_recorder.addHandler(handler)
    logger_recorder.addFilter(filter)
    logger_monitor.addHandler(handler)
    logger_monitor.addFilter(filter)
    logger_common.addHandler(handler)
    logger_common.addFilter(filter)

def get_root_disk_size():
    cmd = r"df -h |grep '/$' |egrep '[0-9]+%' -o |cut -d '%' -f1"
    size = subprocess_open(cmd)
    return size[0]

def get_filename(filenames):
    if iter(filenames) is iter(filenames):  # deny interator!!
        raise TypeError('Must supply a container')
    result = []
    for filename in filenames:
        result.append(filename)
    return result

def delete_file(dirpath, filenames, disk_size=LIMIT_DISK_SIZE):
    if iter(filenames) is iter(filenames):  # deny interator!!
        raise TypeError('Must supply a container')
    if (disk_size > LIMIT_DISK_SIZE):
        for index, filename in enumerate(filenames):
            if (index % 10 == 0) and (int(get_root_disk_size().split('\n')[0]) < LIMIT_DISK_SIZE):
                return
            if os.path.isfile(dirpath + "/" + filename):
                os.remove(dirpath + "/" + filename)
    else:
        logger_monitor.info("ARCHIVE - Don't have to delete files because disk_size({}) is not higher than LIMIT_DISK_SIZE({})".format(disk_size, LIMIT_DISK_SIZE))
#        if os.path.isdir(dirpath):
#            os.rmdir(dirpath)

def compress_file(dirpath, filenames):
    if iter(filenames) is iter(filenames):  # deny interator!!
        raise TypeError('Must supply a container')
    try:
        wtar = tarfile.open(dirpath+'.tar.gz', mode='w:gz')
        for filename in filenames:
            if os.path.isfile(dirpath + "/" + filename):
                wtar.add(dirpath+'/'+filename)
    except Exception as e:
        logger_monitor.error("compress_file() cannot be executed, {}".format(e))
        pass
    finally:
        wtar.close()


# Find process with process name.
def find_process(process_name):
    try:
        cmd_getpid = "ps -ef |grep "+process_name+" | grep -v grep |wc -l"
        ps = subprocess_open(cmd_getpid)
    except Exception as e:
        logger_monitor.error("find_process() cannot be executed, {}".format(e))
        pass
    return ps

# Get logsize of var(SCRIPT_MON_LOG_FOLDER).
def get_logsize():
    try:
        cmd_get_monitorlog_size = "ls -al " + SCRIPT_MON_LOG_FOLDER + \
                                    " | egrep \'" + MON_LOG_FILENAME + \
                                    "$\' |awk \'{print $5}\'"
        monlog_size = subprocess_open(cmd_get_monitorlog_size)
        monlog_size_int = int(monlog_size[0])
    except Exception as e:
        logger_monitor.error("get_logsize() cannot be executed, {}".format(e))
        pass
    return monlog_size_int

#  Rotate logfile when logsize is bigger thant var(LOGSIZE).
def logrotate(logfilepath, logsize):
    try:
        if os.path.isfile(logfilepath+r'.5'):
            os.remove(logfilepath+r'.5')
        if os.path.isfile(logfilepath+r'.4'):
            shutil.copyfile(logfilepath + r'.4', logfilepath + r'.5')
        if os.path.isfile(logfilepath+r'.3'):
            shutil.copyfile(logfilepath + r'.3', logfilepath + r'.4')
        if os.path.isfile(logfilepath+r'.2'):
            shutil.copyfile(logfilepath + r'.2', logfilepath + r'.3')
        if os.path.isfile(logfilepath+r'.1'):
            shutil.copyfile(logfilepath + r'.1', logfilepath + r'.2')
        if os.path.isfile(logfilepath):
            os.rename(logfilepath, logfilepath + r'.1')
        if not os.path.isfile(logfilepath):
            err_file = open(logfilepath, 'w')
            err_file.close()
            logger_monitor.info("File is generated again because of size({})".format(str(logsize)))
            init_logger()
    except Exception as e:
        logger_monitor.error("logrotate() cannot be executed, {}".format(e))
        pass

# Get last month
def get_lastmonth():
    first_day_of_current_month = datetime.today().replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_month_of_current_year = datetime.today().replace(month=1, day=1)
    last_year = first_month_of_current_year - timedelta(days=1)
    return (first_month_of_current_year.year, last_year.year, first_day_of_current_month.month, last_day_of_previous_month.month)

def is_month_begin():
    mod_today = datetime.today() + pd.offsets.MonthBegin(0)
    today = datetime.today()
    return mod_today.day == today.day

# archive_path = /var/log/flows/201608, /var/log/flows/users/201608
def archive_logfolder(compress_path, compress_folder_name, delete_path, delete_folder_name, do_compress):
    _delete_file_path = []
    for i in range(len(delete_path)):
        _delete_file_path.append(delete_path[i] + '.tar.gz')

    # make tarfile, do compress
    if do_compress == True:
        try:
            for compresspath in compress_path:
                filenames = GetFilenames(compresspath)
                compress_file(compresspath, filenames)
        except Exception as e:
            logger_monitor.error("archive tarfile cannot be executed, {}".format(e))
            pass
        else:
            logger_monitor.info("{} is archived successfully!".format(compress_path))
    else:
        logger_monitor.info("Compress option is {}, in order to compress folders, please set do_compress as True.".format(do_compress))

    # delete tarfile
    try:
        for i in range(len(_delete_file_path)):
            if (os.path.isfile(_delete_file_path[i])):
                os.remove(_delete_file_path[i])
    except Exception as e:
        logger_monitor.error("delete tarfile cannot be executed, {}".format(e))
        pass
    else:
        logger_monitor.info("{} is deleted successfully!".format(_delete_file_path[i]))

    # delete files archive period ago
    try:
        for dirpath in delete_path:
            filenames = GetFilenames(dirpath) # get filnames from class
            delete_file(dirpath, filenames, disk_size=int(get_root_disk_size().split('\n')[0]))
    except Exception as e:
        logger_monitor.error("delete files in {} cannot be executed, {}".format(dirpath, e))
        pass
    else:
        logger_monitor.info("files in {} is deleted successfully!".format(dirpath))

def get_archive_month(archive_period):
    # Calculate last_two and last_three month
    try:
        today = datetime.today()
        last_month = today + pd.tseries.offsets.DateOffset(months=-1)
        archiving_month = today + pd.tseries.offsets.DateOffset(months=-archive_period)
    except Exception as e:
        logger_monitor.error("calculate_archive_month() cannot be executed, {}".format(e))
    return {
            'archiving_month.year' : archiving_month.year,
            'archiving_month.month' : archiving_month.month,
            'today.year' : today.year,
            'today.month' : today.month,
            'last_month.year' : last_month.year,
            'last_month.month' : last_month.month,
            }
    #
def archive_rotate_test(do_compress, archive_period):
    if do_compress:
        archive_mon = get_archive_month(archive_period)
        if archive_mon['last_month.month'] < 10:
            compress_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['last_month.year']) + '0' + str(archive_mon['last_month.month']),
                             FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['last_month.year']) + '0' + str(archive_mon['last_month.month'])]
            compress_folder_name = str(archive_mon['last_month.year']) + '0' + str(archive_mon['last_month.month'])
        else:
            compress_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['last_month.year']) + str(archive_mon['last_month.month']),
                             FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['last_month.year']) + str(archive_mon['last_month.month'])]
            compress_folder_name = str(archive_mon['last_month.year']) + str(archive_mon['last_month.month'])
        if archive_mon['archiving_month.month'] < 10:
            delete_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['archiving_month.year']) + '0' + str(archive_mon['archiving_month.month']),
                           FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['archiving_month.year']) + '0' + str(archive_mon['archiving_month.month'])]
            delete_folder_name = str(archive_mon['archiving_month.year']) + '0' + str(archive_mon['archiving_month.month'])
        else:
            delete_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month']),
                           FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month'])]
            delete_folder_name = str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month'])
        print (compress_path)
        print (compress_folder_name)
        print (delete_path)
        print (delete_folder_name)
        archive_logfolder(compress_path, compress_folder_name, delete_path, delete_folder_name, do_compress)
        print ("do make_del_archive_logfolder")

def archive_rotate(do_compress, archive_period):
    global archive_count
    month_count = 0 # values to add to archiving month until last month
    # check disk size and delete folder and files
    if int(get_root_disk_size().split('\n')[0]) > LIMIT_DISK_SIZE:
        while int(get_root_disk_size().split('\n')[0]) > LIMIT_DISK_SIZE:
            try:
                archive_mon = get_archive_month(archive_period)
                # when archiving month is under OCT
                if archive_mon['archiving_month.month'] < 10:
                    delete_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['archiving_month.year']) + '0' + str(archive_mon['archiving_month.month']),
                                FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['archiving_month.year']) + '0' + str(archive_mon['archiving_month.month'])]
                    delete_folder_name = str(archive_mon['archiving_month.year']) + '0' + str(archive_mon['archiving_month.month'])
                    while not (os.path.isdir(delete_path[0]) or os.path.isdir(delete_path[1])):
                        if month_count < archive_period - 1:
                            month_count += 1
                        # get delete path adding month_count
                        if archive_mon['archiving_month.month']+month_count < 10:
                            delete_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['archiving_month.year']) + '0' + str(archive_mon['archiving_month.month']+month_count),
                                        FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['archiving_month.year']) + '0' + str(archive_mon['archiving_month.month']+month_count)]
                            delete_folder_name = str(archive_mon['archiving_month.year']) + '0' + str(archive_mon['archiving_month.month']+month_count)
                        else:
                            delete_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month']+month_count),
                                        FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month']+month_count)]
                            delete_folder_name = str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month']+month_count)
                    # delete files archive month ago
                    try:
                        for dirpath in delete_path:
                            if os.path.isdir(dirpath):
                                filenames = GetFilenames(dirpath) # get filnames from class
                                delete_file(dirpath, filenames, disk_size=int(get_root_disk_size().split('\n')[0]))
                            else:
                                logger_monitor.info("ARCHIVE - There is no folder({})".format(dirpath))
                    except Exception as e:
                        logger_monitor.error("delete files in {} cannot be executed, {}".format(dirpath, e))
                        pass
                # when archiving month is upper than OCT, ex) 10,11,12
                else:
                    delete_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month']),
                                FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month'])]
                    delete_folder_name = str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month'])
                    while not (os.path.isdir(delete_path[0]) or os.path.isdir(delete_path[1])):
                        if month_count < archive_period - 1:
                            month_count += 1
                        # when archiving month is Feb
                        if archive_mon['today.month'] == 2 and archive_mon['archiving_month.month']+month_count == 13:
                            delete_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['today.year']) + '0' + str(archive_mon['today.month']-1),
                                        FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['today.year']) + '0' + str(archive_mon['today.month']-1)]
                            delete_folder_name = str(archive_mon['today.year']) + '0' + str(archive_mon['today.month']-1)
                        # when archiving month is Mar
                        if archive_mon['today.month'] == 3 and archive_mon['archiving_month.month']+month_count == 13:
                            delete_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['today.year']) + '0' + str(archive_mon['today.month']-2),
                                        FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['today.year']) + '0' + str(archive_mon['today.month']-2)]
                            delete_folder_name = str(archive_mon['today.year']) + '0' + str(archive_mon['today.month']-2)
                        if archive_mon['today.month'] == 3 and archive_mon['archiving_month.month']+month_count == 14:
                            delete_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['today.year']) + '0' + str(archive_mon['today.month']-1),
                                        FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['today.year']) + '0' + str(archive_mon['today.month']-1)]
                            delete_folder_name = str(archive_mon['today.year']) + '0' + str(archive_mon['today.month']-1)
                        # when archiving month is Jan
                        delete_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month']+month_count),
                                    FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month']+month_count)]
                        delete_folder_name = str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month']+month_count)
                    # delete files archive month ago
                    try:
                        for dirpath in delete_path:
                            if os.path.isdir(dirpath):
                                filenames = GetFilenames(dirpath) # get filnames from class
                                delete_file(dirpath, filenames, disk_size=int(get_root_disk_size().split('\n')[0]))
                            else:
                                logger_monitor.info("ARCHIVE - There is no folder({})".format(dirpath))
                    except Exception as e:
                        logger_monitor.error("delete files in {} cannot be executed, {}".format(dirpath, e))
                        pass
            except Exception as e:
                logger_monitor.error("disk_size_check and delete files routine cannot be excuted, {}".format(e))
            else:
                logger_monitor.info("ARCHIVE_01 - disk_size_check and delete files routine success!!")
    # for archiving 3 month ago.
    if is_month_begin():
        try:
            archive_mon = get_archive_month(archive_period)
            if archive_mon['last_month.month'] < 10:
                compress_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['last_month.year']) + '0' + str(archive_mon['last_month.month']),
                                 FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['last_month.year']) + '0' + str(archive_mon['last_month.month'])]
                compress_folder_name = str(archive_mon['last_month.year']) + '0' + str(archive_mon['last_month.month'])
            else:
                compress_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['last_month.year']) + str(archive_mon['last_month.month']),
                                 FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['last_month.year']) + str(archive_mon['last_month.month'])]
                compress_folder_name = str(archive_mon['last_month.year']) + str(archive_mon['last_month.month'])
            if archive_mon['archiving_month.month'] < 10:
                delete_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['archiving_month.year']) + '0' + str(archive_mon['archiving_month.month']),
                               FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['archiving_month.year']) + '0' + str(archive_mon['archiving_month.month'])]
                delete_folder_name = str(archive_mon['archiving_month.year']) + '0' + str(archive_mon['archiving_month.month'])
            else:
                delete_path = [FLOW_USER_LOG_FOLDER + '/' + str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month']),
                               FLOW_LOG_FOLDER_PATH + '/' + str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month'])]
                delete_folder_name = str(archive_mon['archiving_month.year']) + str(archive_mon['archiving_month.month'])

            if archive_count == 1:
                archive_logfolder(compress_path, compress_folder_name, delete_path, delete_folder_name, do_compress)
                logger_monitor.info("ARCHIVE - Today is the first day of this month, will start archive if there is folder {} month ago...".format(str(archive_period)))
                archive_count += 1
        except Exception as e:
            logger_monitor.error("archive_rotate() cannot be excuted, {}".format(e))
        else:
            logger_monitor.info("ARCHIVE_02 - Today is not the first day of this month! there is no folder to archive!!!")
    else:
        archive_count = 1

# Execute flow_recorder.py script.
def do_flow_recorder(script_name_path, curTime, process_name):
    try:
        cmd = script_name_path + " &"
        subprocess.Popen(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=True)
    except Exception as e:
        logger_monitor.error("do_flow_recorder() cannot be executed, {}".format(e))
        pass
    else:
        logger_monitor.info("{} process is restarting! check ps -ef |grep {}".format(process_name, process_name))

def start_flow_recorder():
    try:
        cmd = r'sudo /opt/stm/target/c.sh PUT scripts/flow_recorder.py start'
        subprocess.Popen(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=True)
    except Exception as e:
        logger_monitor.error("start_flow_recorder() cannot be executed, {}".format(e))
        pass
    else:
        logger_monitor.info("flow_recorder.py is starting!")

# Get process count by process name.
def get_process_count(process_name):
    try:
        output = find_process(process_name)
        result = output[0]
    except Exception as e:
        logger_monitor.error("get_process_count() cannot be executed, {}".format(e))
        pass
    return result

# Check if current process is working or not.
def compare_process_count(curTime, process_name, recorder_process_count, monitor_process_count):
    try:
        if recorder_process_count == "1\n" or recorder_process_count == "2\n":
            if not os.path.isfile(SCRIPT_MON_LOG_FILE):
                err_file = open(SCRIPT_MON_LOG_FILE, 'w')
                err_file.close()
                logger_monitor.info("Flow {} script is started".format(SCRIPT_FILENAME))
            else:
                logger_monitor.info("{} Process is running.".format(process_name))
                monlog_size = get_logsize()
                if monlog_size > LOGSIZE:
                    logrotate(SCRIPT_MON_LOG_FILE, monlog_size)
                    init_logger()
                else:
                    logger_monitor.info("flow_recorder log size {} is small than default LOGSIZE {}".format(monlog_size, LOGSIZE))
        elif recorder_process_count == "0\n":
            if not os.path.isfile(SCRIPT_MON_LOG_FILE):
                err_file = open(SCRIPT_MON_LOG_FILE, 'w')
                err_file.close()
                logger_monitor.info("Flow {} script is not started".format(SCRIPT_FILENAME))
                logger_monitor.info("Flow process is not started")
                start_flow_recorder()
#                do_flow_recorder(SCRIPT_PATH+SCRIPT_FILENAME, curTime[1], process_name)
                logger_monitor.info("Flow {} script is started".format(SCRIPT_FILENAME))
                logger_monitor.info("Flow {} Process was restarted.".format(SCRIPT_FILENAME))
            else:
                monlog_size = get_logsize()
                if monlog_size > LOGSIZE:
                    logrotate(SCRIPT_MON_LOG_FILE, monlog_size)
                logger_monitor.info("Flow {} process is not running, will restart it".format(SCRIPT_FILENAME))
                start_flow_recorder()
#                do_flow_recorder(SCRIPT_PATH+SCRIPT_FILENAME, curTime[1], process_name)
                logger_monitor.info("Flow {} process was restarted.".format(SCRIPT_FILENAME))
        else:
            logger_monitor.info("process count is too much as expected!")

    except Exception as e:
        logger_monitor.error("compare_process_count() cannot be executed, {}".format(e))
        pass
################################################################################
#                      Flow.Recorder  Module
################################################################################
# Get filepath and command string
def get_filepaths(foldername, INTERFACE_LIST, TOP_NUM, i):
    try:
        save_txt_filepath = FLOW_LOG_FOLDER_PATH + '/' + foldername[0] + \
                            foldername[1] + '/' + foldername[0] + \
                            foldername[1] + foldername[2] + r'_flowinfo_' + \
                            INTERFACE_LIST[i] + r'_' + TOP_NUM + r'.txt'

        save_csv_filepath = FLOW_LOG_FOLDER_PATH + '/' + foldername[0] + \
                            foldername[1] + '/' + foldername[0] + \
                            foldername[1] + foldername[2] + r'_flowinfo_' + \
                            INTERFACE_LIST[i] + r'_' + TOP_NUM + r'.csv'
    except Exception as e:
        logger_recorder.error("get_filepaths() cannot be executed, {}".format(e))
        pass
    return { 'txt':save_txt_filepath, 'csv':save_csv_filepath }



# Create log folder by YearMon
def create_folder(foldername):
    try:
        folder_year_mon = FLOW_LOG_FOLDER_PATH + '/' + foldername[0] + foldername[1]
        folder_user_year_mon = FLOW_USER_LOG_FOLDER + '/' + foldername[0] + foldername[1]

        if not os.path.exists(folder_year_mon):
            os.makedirs(folder_year_mon)
        if not os.path.exists(folder_user_year_mon):
            os.makedirs(folder_user_year_mon)
    except Exception as e:
        logger_recorder.error("create_folder() cannot be executed, {}".format(e))
        pass
# Parsing fieldnames
def parse_fieldnames(data):
    try:
        str_fieldnames = ''
        for i in range(1):
            str_fieldnames = data.splitlines()[1]
        fieldname = str_fieldnames.split()
        fieldnames = []
        for field in fieldname:
            fieldnames.append(re.sub('\"|\'', "", field))
    except Exception as e:
        logger_recorder.error("parse_fieldnames() cannot be executed, {}".format(e))
        pass
    return fieldnames
################################################################################
#       Flowrecorder CLASS
################################################################################
class Flowrecorder:

    def __init__(self, cmd, interface, foldername, logfilepath, logfolderpath, include_subnet_tree):
        self._cmd = cmd
        self._cmd_for_field = re.sub(pattern_for_top, 'top 5', cmd)
        if re.search('source_host', self._cmd):
            src = re.search('[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd)
            self._srchost = self._cmd[src.start():src.end()]
            self._srchost_filepath = '{}/{}_outbound_flow.txt'.format(FLOW_USER_LOG_FOLDER, self._srchost)
        if re.search('dest_host', self._cmd):
            dst = re.search('[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd)
            self._dsthost = self._cmd[dst.start():dst.end()]
            self._dsthost_filepath = '{}/{}_inbound_flow.txt'.format(FLOW_USER_LOG_FOLDER, self._dsthost)
        self.d_interface = interface
        self._ex_interface = self.d_interface['external']
        self._in_interface = self.d_interface['internal']
        self.l_interface = list(interface.values())
        self._foldername = foldername
        self._usersfolder = FLOW_LOG_FOLDER_PATH + r'/users/' + foldername[0] + foldername[1]
        self._frfolder = FLOW_LOG_FOLDER_PATH + r'/' + foldername[0] + foldername[1]
        self._logfilepath = logfilepath
        self._txt_logfilepath = self._logfilepath['txt']
        self._csv_logfilepath = self._logfilepath['csv']
        self._logfolderpath = logfolderpath
        self._include_subnet_tree = include_subnet_tree

    def get_cmd(self):
        return self._cmd

    def get_logfilepath(self):
        return self._logfilepath

    def get_logfolderpath(self):
        return self._logfolderpath
    def printall(self):
        print(self._cmd)
        if re.search('source_host', self._cmd):
            print(self._srchost)
            print(self._srchost_filepath)
        if re.search('dest_host', self._cmd):
            print(self._dsthost)
            print(self._dsthost_filepath)
        print(self._ex_interface)
        print(self._in_interface)
        print(self._foldername)
        print(self._usersfolder)
        print(self._frfolder)
        print(self._logfilepath)
        print(self._logfolderpath)
        print(self._include_subnet_tree)

    def check_error(self, raw_data):
        if raw_data != '':
            for err in err_lists:
                if err in raw_data:
                    err_marked = True
                    err_contents = re.sub(r"\n", "", err)
                    logger_recorder.error('failed from rest cli, msg : {}, CMD : [ {} ]'.format(err_contents, self._cmd))
                    return True
                else:
                    err_marked = False
            if not err_marked:
                return False
        else:
            try:
                raise Exception('NullDataError - raw_data is Null')
            except Exception as e:
                logger_recorder.error('{}'.format(e))
                pass

################################################################################
#       Def : start recording for record_cmd_type
#       0:all, 1:all and users in subnetree, 2:by host, 3:0,1,2
################################################################################
    def start(self, record_file_type, record_cmd_type):
        try:
            _intfs = subprocess_open(get_interface_cmd)
            _intfs = [__intf for __intf in _intfs[0].split('\n') if __intf]
            for _intf in _intfs:
                if re.search(_intf, self._cmd):
                    m = re.search(_intf, self._cmd)
            intf = self._cmd[m.start():m.end()]

            raw_data = subprocess_open(self._cmd)
            err_status = self.check_error(raw_data[0])

            if not (os.path.isdir(self._usersfolder)):
                create_folder(self._foldername)
                if not err_status:
                    logger_recorder.info('success from rest cli, CMD : [ {} ]'.format(self._cmd))
                    if record_cmd_type == 0:
                        self.record_total(raw_data[0], record_file_type, intf)
                    if record_cmd_type == 1:
                        self.record_total(raw_data[0], record_file_type, intf)
                        self.parse_data_by_host(raw_data[0], record_file_type, intf)
                    if record_cmd_type == 2:
                        self.record_total(raw_data[0], record_file_type, intf)
                    if record_cmd_type == 3:
                        self.record_total(raw_data[0], record_file_type, intf)
                        self.parse_data_by_host(raw_data[0], record_file_type, intf)
            else:
                if not err_status:
                    logger_recorder.info('success from rest cli, CMD : [ {} ]'.format(self._cmd))
                    if record_cmd_type == 0:
                        self.record_total(raw_data[0], record_file_type, intf)
                    if record_cmd_type == 1:
                        self.record_total(raw_data[0], record_file_type, intf)
                        self.parse_data_by_host(raw_data[0], record_file_type, intf)
                    if record_cmd_type == 2:
                        self.record_total(raw_data[0], record_file_type, intf)
                    if record_cmd_type == 3:
                        self.record_total(raw_data[0], record_file_type, intf)
                        self.parse_data_by_host(raw_data[0], record_file_type, intf)
        except Exception as e:
            logger_recorder.error("start() cannot be executed, {}".format(e))
            pass
################################################################################
#       Def :  Extract data by HOST # CMD TYPE 3
#              This function is depricated.
################################################################################
    def start_by_host(self, record_file_type):
        try:
            _intfs = subprocess_open(get_interface_cmd)
            for _intf in _intfs[0].split('\n'):
                if re.search(_intf, self._cmd):
                    m = re.search(_intf, self._cmd)
            intf = self._cmd[m.start():m.end()]
            if not (os.path.isdir(self._usersfolder)):
                create_folder(self._foldername)
                raw_data = subprocess_open(self._cmd)

                if 'Cannot connect to server' in raw_data[0]:
                    logger_recorder.error('{} - {}'.format(raw_data[0], self._cmd))
                elif 'does not exist' in raw_data[0]:
                    logger_recorder.error('{} - {}'.format(raw_data[0], self._cmd))
                elif 'no matching objects' in raw_data[0]:
                    logger_recorder.error('{} - {}'.format(raw_data[0], self._cmd))
                else:
                    self.record_total(raw_data[0], record_file_type, intf)
            else:
                raw_data = subprocess_open(self._cmd)
                if 'Cannot connect to server' in raw_data[0]:
                    logger_recorder.error('{} - {}'.format(raw_data[0], self._cmd))
                elif 'does not exist' in raw_data[0]:
                    logger_recorder.error('{} - {}'.format(raw_data[0], self._cmd))
                elif 'no matching objects' in raw_data[0]:
                    logger_recorder.error('{} - {}'.format(raw_data[0], self._cmd))
                else:
                    self.record_total(raw_data[0], record_file_type, intf)
        except Exception as e:
            logger_recorder.error("start_by_host() cannot be executed, {}".format(e))
            pass
################################################################################
#      Def : Write row with generator
#           rows - row from GetRow class's Generator container
#           reader - reader obj from record total()
#           record_file_type - file type trying to write txt or csv
#           fieldnames - fieldnames
#           flow_time - The time extracted from REST API
#           rows_len - Length of rows extracted
#           *args - added for preparing increased parameter,
#           now args is intface's name(intf)
#   return type :
#        cmd_type
#        intf
#        path_of_filename
################################################################################
    def write_row(self, rows, reader, record_file_type, fieldnames, flow_time, rows_len, intf):
        count_values = 1
        labels = []
        labels = fieldnames
        if not re.search('source_host|dest_host', self._cmd):
            cmd_type = r'total'
        if re.search('source_host=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd):
            cmd_type = r'src'
        if re.search('dest_host=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd):
            cmd_type = r'dst'
        for row in rows:
            row['timestamp'] = flow_time
            values = []
            for label in fieldnames:
                values.append(row[label])
            middles = []
            for label in labels:
                middles.append('='*len(label))

            labelLine = list()
            middleLine = list()
            valueLine = list()

            for label, middle, value in zip(labels, middles, values):
                padding = max(len(str(label)), len(str(value)))
                labelLine.append('{0:<{1}}'.format(label, padding))  # generate a string with the variable whitespace padding
                middleLine.append('{0:<{1}}'.format(middle, padding))
                valueLine.append('{0:<{1}}'.format(value, padding))

            # record_file_type = 0 : csv, 1 : txt, 2 : both
            if record_file_type == 1 or record_file_type == 2:
                # cmd type is for total.
                if cmd_type is 'total':
                    if os.path.isfile(self._txt_logfilepath):
                        with open(self._txt_logfilepath, 'a') as fh:
                            fh.write('      '.join(valueLine) + '\r\n')
                        count_values += 1
                        if count_values == rows_len + 1:
                            count_values += 1
                    else:
                        test_file = open(self._txt_logfilepath, 'w')
                        test_file.close()
                        if count_values >= 1 or count_values < rows_len + 1:
                            with open(self._txt_logfilepath, 'a') as fh:
                                fh.write('      '.join(labelLine) + '\r\n')
                        with open(self._txt_logfilepath, 'a') as fh:
                            fh.write('      '.join(valueLine) + '\r\n')
                        count_values += 1
                        if count_values == rows_len + 1:
                            count_values += 1
                # CASE when source_host exist in CMD
                if cmd_type is 'src':
                    src = re.search('source_host=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd)
                    src_host = self._cmd[src.start()+12:src.end()]
                    outbound = intf
                    if outbound in self.d_interface['internal']:
                        filename_by_src_path = "{}/{}{}/{}_outbound_flows.txt".format(FLOW_USER_LOG_FOLDER, self._foldername[0], self._foldername[1], src_host)

                    if os.path.isfile(filename_by_src_path):
                        with open(filename_by_src_path, 'a') as fh:
                            fh.write('      '.join(valueLine) + '\r\n')
                        count_values += 1
                        if count_values == rows_len + 1:
                            count_values += 1
                    else:
                        test_file = open(filename_by_src_path, 'w')
                        test_file.close()
                        if count_values >= 1 or count_values < rows_len + 1:
                            with open(filename_by_src_path, 'a') as fh:
                                fh.write('      '.join(labelLine) + '\r\n')
                        with open(filename_by_src_path, 'a') as fh:
                            fh.write('      '.join(valueLine) + '\r\n')
                        count_values += 1
                        if count_values == rows_len + 1:
                            count_values += 1
                # CASE when dest_host exist in CMD
                if cmd_type is 'dst':
                    dst = re.search('dest_host=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd)
                    dst_host = self._cmd[dst.start()+10:dst.end()]
                    inbound = intf
                    if inbound in self.d_interface['external']:
                        filename_by_dst_path = "{}/{}{}/{}_inbound_flows.txt".format(FLOW_USER_LOG_FOLDER, self._foldername[0], self._foldername[1], dst_host)

                    if os.path.isfile(filename_by_dst_path):
                        with open(filename_by_dst_path, 'a') as fh:
                            fh.write('      '.join(valueLine) + '\r\n')
                        count_values += 1
                        if count_values == rows_len + 1:
                            count_values += 1
                    else:
                        test_file = open(filename_by_dst_path, 'w')
                        test_file.close()
                        if count_values >= 1 or count_values < rows_len + 1:
                            with open(filename_by_dst_path, 'a') as fh:
                                fh.write('      '.join(labelLine) + '\r\n')
                        with open(filename_by_dst_path, 'a') as fh:
                            fh.write('      '.join(valueLine) + '\r\n')
                        count_values += 1
                        if count_values == rows_len + 1:
                            count_values += 1
                # record_file_type = 0 : csv, 1 : txt, 2 : both
                if record_file_type == 0 or record_file_type == 2:
                    # case record total
                    if cmd_type is 'total':
                        if not (os.path.isfile(self._csv_logfilepath)):
                            csv_file = open(self._csv_logfilepath, 'w')
                            csv_file.close()
                            with open(self._csv_logfilepath, "a") as fh:
                                fh.write(','.join(fieldnames))
                                fh.write('\n')
                                writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                                writer.writerow(row)
                        else:
                            with open(self._csv_logfilepath, "a") as fh:
                                writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                                writer.writerow(row)
                    # CASE when source_host exist in CMD
                    if cmd_type is 'src':
                        src = re.search('source_host=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd)
                        src_host = self._cmd[src.start()+12:src.end()]
                        outbound = intf
                        if outbound in self.d_interface['internal']:
                            filename_by_src_path = "{}/{}{}/{}_outbound_flows.csv".format(FLOW_USER_LOG_FOLDER, self._foldername[0], self._foldername[1], src_host)
                        if not (os.path.isfile(filename_by_src_path)):
                            csv_file = open(filename_by_src_path, 'w')
                            csv_file.close()
                            with open(filename_by_src_path, "a") as fh:
                                fh.write(','.join(fieldnames))
                                fh.write('\n')
                                fh.write('{},'.format(flow_time))
                                writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                                writer.writerow(row)
                        else:
                            with open(filename_by_src_path, "a") as fh:
                                fh.write('{},'.format(flow_time))
                                writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                                writer.writerow(row)
                    # CASE when dest_host exist in CMD
                    if cmd_type is 'dst':
                        dst = re.search('dest_host=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd)
                        dst_host = self._cmd[dst.start()+10:dst.end()]
                        inbound = intf
                        if inbound in self.d_interface['external']:
                            filename_by_dst_path = "{}/{}{}/{}_inbound_flows.csv".format(FLOW_USER_LOG_FOLDER, self._foldername[0], self._foldername[1], dst_host)
                        if not (os.path.isfile(filename_by_dst_path)):
                            csv_file = open(filename_by_dst_path, 'w')
                            csv_file.close()
                            with open(filename_by_dst_path, "a") as fh:
                                fh.write(','.join(fieldnames))
                                fh.write('\n')
                                writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                                writer.writerow(row)
                        else:
                            with open(filename_by_dst_path, "a") as fh:
                                writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                                writer.writerow(row)
        if (cmd_type is 'total'):
            if record_file_type == 1 or record_file_type == 2:
                return [cmd_type, intf, self._txt_logfilepath]
            if record_file_type == 0 or record_file_type == 2:
                return [cmd_type, intf, self._csv_logfilepath]
        elif (cmd_type is 'src'):
            if record_file_type == 1 or record_file_type == 2:
                return [cmd_type, intf, filename_by_src_path]
            if record_file_type == 0 or record_file_type == 2:
                return [cmd_type, intf, filename_by_src_path]
        elif (cmd_type is 'dst'):
            if record_file_type == 1 or record_file_type == 2:
                return [cmd_type, intf, filename_by_dst_path]
            if record_file_type == 0 or record_file_type == 2:
                return [cmd_type, intf, filename_by_dst_path]
        else:
            if record_file_type == 1 or record_file_type == 2:
                return [cmd_type, intf, self._txt_logfilepath]
            if record_file_type == 0 or record_file_type == 2:
                return [cmd_type, intf, self._csv_logfilepath]
################################################################################
#      Def : extract total from the cmd
################################################################################
    def record_total(self, raw_data, record_file_type, intf):
        """
        DEF record_total is the function that records file from the cmd into csv and txt
        raw_data_row - the raw data,
        record_file_type - type for txt or csv,
        args[0] - interface name
        """
        try:
            m = re.search(pattern, raw_data)
            flow_time = raw_data[m.start():m.end()]
            # Get fieldnames
            fieldnames = parse_fieldnames(raw_data)
            # Make field pattern
            pattern_02 = ''
            for field in fieldnames:
                pattern_02 += field+"|"

            raw_data = re.sub(pattern_01, "", raw_data)
            raw_data = re.sub(pattern_03, "", raw_data)
            raw_data = re.sub(pattern_04, "", raw_data)
            raw_data = re.sub(pattern_02, "", raw_data)
            raw_data = re.sub(pattern_03, "", raw_data)

            reader = csv.DictReader(raw_data.splitlines(),
                                    delimiter=' ',
                                    skipinitialspace=True,
                                    fieldnames=fieldnames)

            result = sorted(reader, key=lambda d: d['srchost'])
            rows_len = len(result)
            fieldnames.insert(0, 'timestamp')
            rows = GetRow(result)
            try:
                write_result = self.write_row(rows, reader, record_file_type, fieldnames, flow_time, rows_len, intf)
            except Exception as e:
                logger_recorder.error("write_row() cannot be executed, {}".format(e))
            else:
                logger_recorder.info('Flow info from interfaces {} is extracted to {} successfully!'.format(write_result[1], write_result[2]))
        except Exception as e:
            logger_recorder.error("record_total() cannot be executed, {}".format(e))
            pass
################################################################################
#       Def : Extract data by subnetree
################################################################################
    def parse_data_by_host(self, raw_data, record_file_type, *args):
        """
        DEF parse_data_by_host is the function that parses the raw_data and
        record data into csv and txt files.
        raw_data_row - raw data,
        record_file_type -  type for txt or csv,
        args[0] - interface name
        """
        try:
            m = re.search(pattern, raw_data)
            startidx = m.start()
            endidx = m.end()

            flow_time = raw_data[startidx:endidx]

            # Get fieldnames
            fieldnames = parse_fieldnames(raw_data)

            # Make field pattern
            pattern_02 = ''
            for field in fieldnames:
                pattern_02 += field+"|"

            raw_data = re.sub(pattern_01, "", raw_data)
            raw_data = re.sub(pattern_03, "", raw_data)
            raw_data = re.sub(pattern_04, "", raw_data)
            raw_data = re.sub(pattern_02, "", raw_data)
            raw_data = re.sub(pattern_03, "", raw_data)

            reader = csv.DictReader(raw_data.splitlines(),
                                    delimiter=' ',
                                    skipinitialspace=True,
                                    fieldnames=fieldnames)

            result = sorted(reader, key=lambda d: d['srchost'])
            # get length of rows
            rows_len = len(result)
            # set for aligning txt
            count_values = 1
            labels = []
            labels = fieldnames
            fieldnames.insert(0, 'timestamp')
            # do log for the users
            for row in result:
                row['timestamp'] = flow_time
                # for aligning txt
                values = []
                for label in fieldnames:
                    values.append(row[label])
                middles = []
                for label in labels:
                    middles.append('='*len(label))

                labelLine = list()
                middleLine = list()
                valueLine = list()

                for label, middle, value in zip(labels, middles, values):
                    padding = max(len(str(label)), len(str(value)))
                    labelLine.append('{0:<{1}}'.format(label, padding))  # generate a string with the variable whitespace padding
                    middleLine.append('{0:<{1}}'.format(middle, padding))
                    valueLine.append('{0:<{1}}'.format(value, padding))
                # EXTERNAL, this case, stm9
                for ex_int in self._ex_interface:
                    if row['in_if'] == ex_int:
                        if row['dsthost'] in self._include_subnet_tree:
                            flowlog_csv_by_dsthost_path = self._logfolderpath + row['dsthost'] + '-' + row['in_if'] + '-inbound.csv'
                            flowlog_txt_by_dsthost_path = self._logfolderpath + row['dsthost'] + '-' + row['in_if'] + '-inbound.txt'
                            # Parse CSV if row's in_if is STM9 and record_file_type = 0 :csv, 1 :txt, 2 :both
                            if record_file_type == 0 or record_file_type == 2:
                                if not (os.path.isfile(flowlog_csv_by_dsthost_path)):
                                    csv_file = open(flowlog_csv_by_dsthost_path, 'w')
                                    csv_file.close()
                                    with open(flowlog_csv_by_dsthost_path, "a") as fh:
                                        fh.write(','.join(fieldnames))
                                        fh.write('\r\n')
                                        writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                                        writer.writerow(row)
                                else:
                                    with open(flowlog_csv_by_dsthost_path, "a") as fh:
                                        writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                                        writer.writerow(row)
                            # Parse TXT if row's in_if is STM9
                            if record_file_type == 1 or record_file_type == 2:
                                #
                                if os.path.isfile(flowlog_txt_by_dsthost_path):
                                    with open(flowlog_txt_by_dsthost_path, 'a') as fh:
                                        fh.write('      '.join(valueLine) + '\r\n')
                                    count_values += 1
                                    if count_values == rows_len + 1:
                                        count_values += 1
                                else:
                                    txt_file = open(flowlog_txt_by_dsthost_path, 'w')
                                    txt_file.close()
                                    if count_values >= 1 or count_values < rows_len + 1:
                                        with open(flowlog_txt_by_dsthost_path, 'a') as fh:
                                            fh.write('      '.join(labelLine) + '\r\n')
                                    with open(flowlog_txt_by_dsthost_path, 'a') as fh:
                                        fh.write('      '.join(valueLine) + '\r\n')
                                    count_values += 1
                                    if count_values == rows_len + 1:
                                        count_values += 1
                                #

                # INTERNAL, this case, stm10
                for in_int in self._in_interface:
                    if row['in_if'] == in_int:
                        if row['srchost'] in self._include_subnet_tree:
                            flowlog_csv_by_srchost_path = self._logfolderpath + row['srchost'] + '-' + row['in_if'] + '-outbound.csv'
                            flowlog_txt_by_srchost_path = self._logfolderpath + row['srchost'] + '-' + row['in_if'] + '-outbound.txt'
                            # Parse CSV if row's in_if is STM10
                            if record_file_type == 0 or record_file_type == 2:
                                if not (os.path.isfile(flowlog_csv_by_srchost_path)):
                                    csv_file = open(flowlog_csv_by_srchost_path, 'w')
                                    csv_file.close()
                                    with open(flowlog_csv_by_srchost_path, "a") as fh:
                                        fh.write(','.join(fieldnames))
                                        fh.write('\r\n')
                                        writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                                        writer.writerow(row)
                                else:
                                    with open(flowlog_csv_by_srchost_path, "a") as fh:
                                        #fh.write('{}'.format(flow_time))
                                        writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                                        writer.writerow(row)
                            # Parse TXT if row's in_if is STM10
                            if record_file_type == 1 or record_file_type == 2:
                                #
                                if os.path.isfile(flowlog_txt_by_srchost_path):
                                    with open(flowlog_txt_by_srchost_path, 'a') as fh:
                                        fh.write('      '.join(valueLine) + '\r\n')
                                    count_values += 1
                                    if count_values == rows_len + 1:
                                        count_values += 1
                                else:
                                    txt_file = open(flowlog_txt_by_srchost_path, 'w')
                                    txt_file.close()
                                    if count_values >= 1 or count_values < rows_len + 1:
                                        with open(flowlog_txt_by_srchost_path, 'a') as fh:
                                            fh.write('      '.join(labelLine) + '\r\n')
                                    with open(flowlog_txt_by_srchost_path, 'a') as fh:
                                        fh.write('      '.join(valueLine) + '\r\n')
                                    count_values += 1
                                    if count_values == rows_len + 1:
                                        count_values += 1

        except Exception as e:
            logger_recorder.error("parse_data_by_host() cannot be executed, {}".format(e))
            pass
        else:
            logger_recorder.info('Flow info by host from interfaces {} is extracted to {} successfully!'.format(args[0], FLOW_USER_LOG_FOLDER))
