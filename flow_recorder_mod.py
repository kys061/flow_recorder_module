import os
import subprocess
from datetime import datetime, timedelta
import time
import re
import csv
import itertools
import logging
import tarfile
import pandas as pd
from SubnetTree import SubnetTree

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
LOGSIZE = 50000000 # 1000 = 1Kbyte, 1000000 = 1Mbyte, 50000000 = 50Mbyte
archive_count = 1

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
pattern_01 = r'[-]{1,10}'
pattern_03 = r'\s+\n'
pattern_04 = r'Flows at [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
#
is_extracted = False
#
INCLUDE = [
        '101.250.240.0/24',
        '101.250.241.0/24',
        '192.168.0.0/16',
        '10.0.0.0/8',
        '172.16.0.0/16'
        '101.250.242.0/24',
        '203.212.0.0/16'
]
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
################################################################################
#                      Flow.Monitor  Class
################################################################################
class GetFilenames(object):
    def __init__(self, dirpath):
        self._dirpath = dirpath
    def __iter__(self):
        filelist = os.listdir(self._dirpath)
        for filename in filelist:
            yield filename

################################################################################
#                      Flow.Monitor  Module
################################################################################
def get_filename(filenames):
    if iter(filenames) is iter(filenames):  # deny interator!!
        raise TypeError('Must supply a container')
    result = []
    for filename in filenames:
        result.append(filename)
    return result

def delete_file(dirpath, filenames):
    if iter(filenames) is iter(filenames):  # deny interator!!
        raise TypeError('Must supply a container')
    for filename in filenames:
        os.remove(dirpath + "/" + filename)
    if os.path.isdir(dirpath):
        os.rmdir(dirpath)

def compress_file(dirpath, filenames):
    if iter(filenames) is iter(filenames):  # deny interator!!
        raise TypeError('Must supply a container')
    try:
        wtar = tarfile.open(dirpath+'.tar.gz', mode='w:gz')
        for filename in filenames:
            wtar.add(dirpath+'/'+filename)
    except Exception as e:
        logger_monitor.error("compress_file() cannot be executed, {}".format(e))
        pass
    else:
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
        if os.path.isfile(logfilepath+".5"):
            os.remove(logfilepath+".5")
        if os.path.isfile(logfilepath+".4"):
            shutil.copyfile(logfilepath + r'.4', logfilepath + r'.5')
        if os.path.isfile(logfilepath+".3"):
            shutil.copyfile(logfilepath + r'.3', logfilepath + r'.4')
        if os.path.isfile(logfilepath+".2"):
            shutil.copyfile(logfilepath + r'.2', logfilepath + r'.3')
        if os.path.isfile(logfilepath+".1"):
            shutil.copyfile(logfilepath + r'.1', logfilepath + r'.2')
        if os.path.isfile(logfilepath):
            os.rename(logfilepath, logfilepath + r'.1')
        if not os.path.isfile(logfilepath):
            err_file = open(logfilepath, 'w')
            err_file.close()
            logger_monitor.info("File is generated again because of size({})".format(str(logsize)))
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
    #print("make archive files in {}!!!".format(archive_path))
    #_compress_file_name = compress_folder_name + '.tar.gz'
    _delete_file_path = []
    for i in range(len(delete_path)):
        _delete_file_path.append(delete_path[i] + '.tar.gz')
    #print("{}, {}".format(_archive_file_name, _archive_folder_name))

    # make tarfile, do compress
    if do_compress == True:
        try:
            for compresspath in compress_path:
                filenames = GetFilenames(compresspath)
                compress_file(compresspath, filenames)
            #for i in range(len(delete_path)):
            #    wtar = tarfile.open(delete_path[i]+'.tar.gz', mode='w:gz')
            #    wtar.add(delete_path[i])
            #    wtar.close()
            #for i in range(len(compress_path)):
            #    wtar = tarfile.open(compress_path[i]+'.tar.gz', mode='w:gz')
            #    wtar.add(compress_path[i])
            #    wtar.close()
            #    print("Compress is success at {}!!".format(compress_path[i]))
        except Exception as e:
            logger_monitor.error("archive tarfile cannot be executed, {}".format(e))
            pass
        else:
            logger_monitor.info("{} is archived successfully!".format(compress_path))
    else:
        logger_monitor.info("Compress option is {}, in order to compress folders, please set do_compress as True.".format(do_compress))

    try:
       # delete tarfile
        for i in range(len(_delete_file_path)):
            if (os.path.isfile(_delete_file_path[i])):
                os.remove(_delete_file_path[i])
    except Exception as e:
        logger_monitor.error("delete tarfile cannot be executed, {}".format(e))
        pass
    else:
        logger_monitor.info("{} is deleted successfully!".format(_delete_file_path[i]))

    try:
        # delete files archive period ago
        for dirpath in delete_path:
            filenames = GetFilenames(dirpath) # get filnames from class
            delete_file(dirpath, filenames)
    except Exception as e:
        logger_monitor.error("delete files in {} cannot be executed, {}".format(dirpath, e))
        pass
    else:
        logger_monitor.info("files in {} is deleted successfully!".format(dirpath))


    #if tarfile.is_tarfile(FLOW_USER_LOG_FOLDER+'/'+_archive_file_name):
    #    return True
    #else:
    #    return False
def get_archive_month(archive_period):
    # Calculate last_two and last_three month
    try:
        #archive_period = 4
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
    #logger_monitor.info("Today is the first day of this month, will start archive if there is folder {} month ago...".format(str(archive_period)))

def archive_rotate(do_compress, archive_period):
    try:
        global archive_count
        if is_month_begin():
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
                logger_monitor.info("Today is the first day of this month, will start archive if there is folder {} month ago...".format(str(archive_period)))
                archive_count += 1
        else:
            archive_count = 1
            logger_monitor.info("Today is not the first day of this month! there is no folder to archive!!!")
    except Exception as e:
        logger_monitor.error("archive_rotate() cannot be excuted, {}".format(e))

def archive_rotate_mod(do_compress, archive_period):
    global archive_count
    if is_month_begin():
        current_year, last_year, current_month, last_month = get_lastmonth()
        # Calculate last_two and last_three month
        if current_month == 1:
            last_three_month = 10
            last_two_month = 11
        elif current_month == 2:
            last_three_month = 11
            last_two_month = 12
        elif current_month == 3:
            last_three_month = 12
            last_two_month = 1
        else:
            last_three_month = current_month - 3
            last_two_month = current_month - 2
        # Change Int to Str
        if current_month >= 1 or current_month < 10:
            current_month = '0'+str(current_month)
        else:
            current_month = str(current_month)

        if last_month >= 1 or last_month < 10:
            last_month = '0'+str(last_month)
        else:
            last_month = str(last_month)

        if last_two_month >= 1 or last_two_month < 10:
            last_two_month = '0'+str(last_two_month)
        else:
            last_two_month = str(last_two_month)

        if last_three_month >= 1 or last_three_month < 10:
            last_three_month = '0'+str(last_three_month)
        else:
            last_three_month = str(last_three_month)
        current_year = str(current_year)
        last_year = str(last_year)
        # Make archive path and floder name
        if str(current_month) == '01':
            archive_path = [FLOW_USER_LOG_FOLDER + '/' + str(last_year) + str(last_month),
                            FLOW_LOG_FOLDER_PATH + '/' + str(last_year) + str(last_month)]
            archive_folder_name = str(last_year) + str(last_month)
        else:
            archive_path = [FLOW_USER_LOG_FOLDER + '/' + str(current_year) + str(last_month) + '/',
                            FLOW_LOG_FOLDER_PATH + '/' + str(last_year) + str(last_month) + '/']
            archive_folder_name = str(current_year) + str(last_month)
        if str(current_month) == '01' or str(current_month) == '02' or str(current_month) == '03':
            delete_path = [FLOW_USER_LOG_FOLDER + '/' + str(last_year) + str(last_three_month) + '/',
                           FLOW_LOG_FOLDER_PATH + '/' + str(last_year) + str(last_three_month) + '/']
            delete_folder_name = str(last_year) + str(last_three_month)
        else:
            delete_path = [FLOW_USER_LOG_FOLDER + '/' + str(current_year) + str(last_three_month),
                           FLOW_LOG_FOLDER_PATH + '/' + str(current_year) + str(last_three_month)]
            delete_folder_name = str(current_year) + str(last_three_month)

        #print ("{}, {}, {}, {}, {}, {} -> archive last_month folder".format(
            #current_year, last_year, current_month, last_month, last_three_month,
            #archive_path, archive_folder_name, delete_path, delete_folder_name))
        # Make tar.gz file and delete file last three month ago
        if archive_count == 1:
            archive_logfolder(archive_path, archive_folder_name, delete_path, delete_folder_name, do_compress)
            logger_monitor.info("Today is the first day of this month, will start archive if there is folder 3 month ago...")
            archive_count += 1
            #if make_del_archive_logfolder(archive_path, archive_folder_name, delete_path, delete_folder_name, has_archive):
            #    logger_monitor.info("Today is the first day of this month, will start archive if there is folder 3 month ago...")
                #print("archive_count : %d " % archive_count)
            #    archive_count += 1
            #    logger_monitor.info("{} is archived successfully!".format(archive_folder_name))
            #else:
            #    logger_monitor.info("Today is the first day of this month, will start archive if there is folder 3 month ago...")
                #print("archive_count : %d " % archive_count)
            #    archive_count += 1
            #    logger_monitor.error("{} is not archived!".format(archive_folder_name))
    else:
        logger_monitor.info("Today is not the first day of this month!")
        logger_monitor.info("There is no folder to archive!")
        #print("archive_count : %d " % archive_count)
        #print ("not archive last_month folder")
        archive_count = 1

# Execute flow_recorder.py script.
def do_flow_recorder(script_name_path, curTime, process_name):
    try:
        cmd = "sudo " + script_name_path + " &"
        subprocess.Popen(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=True)
    except Exception as e:
        logger_monitor.error("do_flow_recorder() cannot be executed, {}".format(e))
        pass
    else:
        logger_monitor.info("{} process is restarting! check ps -ef |grep {}".format(process_name, process_name))


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
        if recorder_process_count == "1\n":
            if not os.path.isfile(SCRIPT_MON_LOG_FILE):
                err_file = open(SCRIPT_MON_LOG_FILE, 'w')
                err_file.close()
                logger_monitor.info("Flow {} script is started".format(SCRIPT_FILENAME))
            else:
                logger_monitor.info("{} Process is running.".format(process_name))
                monlog_size = get_logsize()
                if monlog_size > LOGSIZE:
                    logrotate(SCRIPT_MON_LOG_FILE, monlog_size)
        elif recorder_process_count == "0\n":
            if not os.path.isfile(SCRIPT_MON_LOG_FILE):
                err_file = open(SCRIPT_MON_LOG_FILE, 'w')
                err_file.close()
                logger_monitor.info("Flow {} script is not started".format(SCRIPT_FILENAME))
                logger_monitor.info("Flow process is not started")
                do_flow_recorder(SCRIPT_PATH+SCRIPT_FILENAME, curTime[1], process_name)
                logger_monitor.info("Flow {} script is started".format(SCRIPT_FILENAME))
                logger_monitor.info("Flow {} Process was restarted.".format(SCRIPT_FILENAME))
            else:
                monlog_size = get_logsize()
                if monlog_size > LOGSIZE:
                    logrotate(monlog_size)
                logger_monitor.info("Flow {} process is not running, will restart it".format(SCRIPT_FILENAME))
                do_flow_recorder(SCRIPT_PATH+SCRIPT_FILENAME, curTime[1], process_name)
                logger_monitor.info("Flow {} process was restarted.".format(SCRIPT_FILENAME))
        else:
            pass
    except Exception as e:
        logger_monitor.error("compare_process_count() cannot be executed, {}".format(e))
        pass
################################################################################

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
        #print(str_fieldnames)
        fieldname = str_fieldnames.split()
        fieldnames = []
        for field in fieldname:
            fieldnames.append(re.sub('\"|\'', "", field))
    except Exception as e:
        logger_recorder.error("parse_fieldnames() cannot be executed, {}".format(e))
        pass
    return fieldnames
#
def makeSubnetTree(data_name):
    result = SubnetTree()
    data = eval(data_name)
    if isinstance(data, dict):
        for cidr, name in data.iteritems():
            result[cidr] = name
    else:
        for cidr in data:
            result[cidr] = str(cidr)
    return result
################################################################################

################################################################################
#       Flowrecorder CLASS
################################################################################
class Flowrecorder:

    def __init__(self, cmd, interface, foldername, logfilepath, logfolderpath, include_subnet_tree):
#        global INCLUDE
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
#        INCLUDE = include

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

################################################################################
#       all users CMD TYPE 2
################################################################################
    def start_fr_by_host(self, record_file_type):
        try:
            m = re.search('stm[0-9]+', self._cmd)
            intf = self._cmd[m.start():m.end()]
            if not (os.path.isdir(self._usersfolder)):
                create_folder(self._foldername)
                #fh = open(self._logfilepath, 'w')
                #fh.close()
                raw_data = subprocess_open(self._cmd)

                if 'Cannot connect to server' in raw_data[0]:
                    logger_recorder.error('{} - {}'.format(raw_data[0], self._cmd))
                elif 'does not exist' in raw_data[0]:
                    logger_recorder.error('{} - {}'.format(raw_data[0], self._cmd))
                elif 'no matching objects' in raw_data[0]:
                    logger_recorder.error('{} - {}'.format(raw_data[0], self._cmd))
                else:
                    #fieldnames = parse_fieldnames(raw_data[0])
                    #fieldnames.insert(0, 'timestamp')
                    #with open(self._logfilepath, "a") as fh:
                        #fh.write(','.join(fieldnames))
                        #fh.write('\r\n')
                    #time.sleep(1)
                    self.parse_data_by_host(raw_data[0], record_file_type, intf)
                    #logger_recorder.info('Flow info by host from interfaces {} is extracted to {} successfully!'.format(intf, FLOW_USER_LOG_FOLDER))
            else:
                raw_data = subprocess_open(self._cmd)
                if 'Cannot connect to server' in raw_data[0]:
                    logger_recorder.error('{} - {}'.format(raw_data[0], self._cmd))
                elif 'does not exist' in raw_data[0]:
                    logger_recorder.error('{} - {}'.format(raw_data[0], self._cmd))
                elif 'no matching objects' in raw_data[0]:
                    logger_recorder.error('{} - {}'.format(raw_data[0], self._cmd))
                else:
                    self.parse_data_by_host(raw_data[0], record_file_type, intf)
                    #logger_recorder.info('Flow info by host from interfaces {} is extracted to {} successfully!'.format(intf, FLOW_USER_LOG_FOLDER))
        except Exception as e:
            logger_recorder.error("do_csv_log() cannot be executed, {}".format(e))
            pass

################################################################################
#       make subnet tree
################################################################################
    def makeSubnetTree(self, data_name):
        result = SubnetTree()
        data = eval(data_name)
        if isinstance(data, dict):
            for cidr, name in data.iteritems():
                result[cidr] = name
        else:
            for cidr in data:
                result[cidr] = str(cidr)
        return result
################################################################################
#       all users CMD TYPE 2
################################################################################
    def parse_data_by_host(self, raw_data, record_file_type, *args):
        """
        def parse is the function that parses raw data from the shell into csv and txt
        csv_data_row is the raw data,
        csv_filepath is the path for csv,
        save_csv_users_folder is the path for users(srchost or dsthost)
        """
        try:
            m = re.search(pattern, raw_data)
            startidx = m.start()
            endidx = m.end()
            csv_time = raw_data[startidx:endidx]

            # Get fieldnames
            fieldnames = parse_fieldnames(raw_data)

            # Make field pattern
            pattern_02 = ''
            for field in fieldnames:
                pattern_02 += field+"|"

            raw_data = re.sub(pattern, "", raw_data)
            raw_data = re.sub(pattern_01, "", raw_data)
            raw_data = re.sub(pattern_02, "", raw_data)
            raw_data = re.sub(pattern_03, "\r\n", raw_data)
            reader = csv.DictReader(itertools.islice(raw_data.splitlines(), 1,
                                                     None),
                                    delimiter=' ',
                                    skipinitialspace=True,
                                    fieldnames=fieldnames)

            result = sorted(reader, key=lambda d: d['srchost'])
            #include = self._include
            #include_subnet_tree = makeSubnetTree('INCLUDE')
            # for
            count_values = 1
            labels = []
            labels = fieldnames
            # do log for the users
            for row in result:
                values = []
                for label in fieldnames:
                    values.append(row[label])
                middles = []

    #            value = []
    #            for label in fieldnames:
    #                value.append(row[label])
    #            print value

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
                # Add datetime
                timestamp = 'timestamp'
                labelLine.insert(0, '{0:<{1}}'.format(timestamp, len(str(csv_time))))
                middleLine.insert(0, '{0:<{1}}'.format('='*len(timestamp), len(str(csv_time))))
                valueLine.insert(0, '{0:<{1}}'.format(csv_time, len(str(csv_time))))

    #            if count_values == 1:
    #                print ('{} length of each result -> {}\r'.format(title_time, len(result)))
    #                print ('\t'.join(labelLine) + '\r')
    #                print ('\t'.join(middleLine) + '\n')

    #            print ('\t'.join(valueLine)+'\r')
    #            count_values += 1

    #            if count_values == len(result)+1:
    #                count_values = 1
################################################################################
#       EXTERNAL, this case, stm9
################################################################################
                if row['in_if'] == self._ex_interface:
                    if row['dsthost'] in self._include_subnet_tree:
                        flowlog_csv_by_dsthost_path = self._logfolderpath + row['dsthost'] + '-' + row['in_if'] + '-inbound.csv'
                        flowlog_txt_by_dsthost_path = self._logfolderpath + row['dsthost'] + '-' + row['in_if'] + '-inbound.txt'
################################################################################
#       Parse CSV if row's in_if is STM9 and record_file_type = 0 : csv, 1 : txt
#       2 : both
################################################################################
                        if record_file_type == 0 or record_file_type == 2:
                            if not (os.path.isfile(flowlog_csv_by_dsthost_path)):
                                csv_file = open(flowlog_csv_by_dsthost_path, 'w')
                                csv_file.close()
                                with open(flowlog_csv_by_dsthost_path, "a") as fh:
                                    fh.write(','.join(fieldnames))
                                    fh.write('\r\n')
                                    fh.write('{},'.format(csv_time))
                                    writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                                    writer.writerow(row)
                            else:
                                with open(flowlog_csv_by_dsthost_path, "a") as fh:
                                    fh.write('{},'.format(csv_time))
                                    writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                                    writer.writerow(row)
################################################################################
#       Parse TXT if row's in_if is STM9
################################################################################
                        if record_file_type == 1 or record_file_type == 2:
                            if not (os.path.isfile(flowlog_txt_by_dsthost_path)):
                                txt_file = open(flowlog_txt_by_dsthost_path, 'w')
                                txt_file.close()
                                if count_values >= 1 or count_values < len(result)+1:
                                    with open(flowlog_txt_by_dsthost_path, "a") as fh:
                                        fh.write('    '.join(labelLine) + '\r\n')
                                        #fh.write('    '.join(middleLine) + '\r\n')
                                with open(flowlog_txt_by_dsthost_path, "a") as fh:
                                    fh.write('    '.join(valueLine)+'\r\n')
                                count_values += 1

                                if count_values == len(result)+1:
                                    count_values = 1
                            else:
                                with open(flowlog_txt_by_dsthost_path, "a") as fh:
                                    fh.write('    '.join(valueLine)+'\r\n')
                                count_values += 1
                                if count_values == len(result)+1:
                                    count_values = 1
################################################################################
#       INTERNAL, this case, stm10
################################################################################
                elif row['in_if'] == self._in_interface:
                    if row['srchost'] in self._include_subnet_tree:
                        flowlog_csv_by_srchost_path = self._logfolderpath + row['srchost'] + '-' + row['in_if'] + '-outbound.csv'
                        flowlog_txt_by_srchost_path = self._logfolderpath + row['srchost'] + '-' + row['in_if'] + '-outbound.txt'
################################################################################
#       Parse CSV if row's in_if is STM10
################################################################################
                        if record_file_type == 0 or record_file_type == 2:
                            if not (os.path.isfile(flowlog_csv_by_srchost_path)):
                                csv_file = open(flowlog_csv_by_srchost_path, 'w')
                                csv_file.close()
                                with open(flowlog_csv_by_srchost_path, "a") as fh:
                                    fh.write(','.join(fieldnames))
                                    fh.write('\r\n')
                                    fh.write('{},'.format(csv_time))
                                    writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                                    writer.writerow(row)
                            else:
                                with open(flowlog_csv_by_srchost_path, "a") as fh:
                                    fh.write('{},'.format(csv_time))
                                    writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                                    writer.writerow(row)
################################################################################
#       Parse TXT if row's in_if is STM10
################################################################################
                        if record_file_type == 1 or record_file_type == 2:
                            if not (os.path.isfile(flowlog_txt_by_srchost_path)):
                                txt_file = open(flowlog_txt_by_srchost_path, 'w')
                                txt_file.close()
                                if count_values >= 1 or count_values < len(result)+1:
                                    with open(flowlog_txt_by_srchost_path, "a") as output:
                                        output.write('    '.join(labelLine) + '\r\n')
                                        #output.write('    '.join(middleLine) + '\r\n')
                                with open(flowlog_txt_by_srchost_path, "a") as output:
                                    output.write('    '.join(valueLine)+'\r\n')
                                count_values += 1

                                if count_values == len(result)+1:
                                    count_values = 1
                            else:
                                with open(flowlog_txt_by_srchost_path, "a") as output:
                                    output.write('    '.join(valueLine)+'\r\n')
                                count_values += 1

                                if count_values == len(result)+1:
                                    count_values = 1
                else:
                    pass
        except Exception as e:
            logger_recorder.error("parse_data_by_host() cannot be executed, {}".format(e))
            pass
        else:
            logger_recorder.info('Flow info by host from interfaces {} is extracted to {} successfully!'.format(args[0], FLOW_USER_LOG_FOLDER))

################################################################################
#       Write the txt type log from the command type 3
################################################################################
    def start_fr_txt(self):
        try:
            global is_extracted
            #pattern = r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
            #pattern_01 = r'[-]{1,10}'
            #pattern_03 = r'\s+\n'
            #pattern_04 = r'Flows at [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'

            raw_data = subprocess_open(self._cmd_for_field)
            # ERR Routine
            if 'Cannot connect to server' in raw_data[0]:
                logger_recorder.error("Cannot parse fieldname due to - {}".format(raw_data[0]))
            elif 'does not exist' in raw_data[0]:
                logger_recorder.error("Cannot parse fieldname due to - {}".format(raw_data[0]))
            elif 'no matching objects' in raw_data[0]:
                logger_recorder.error("Cannot parse fieldname due to - {}".format(raw_data[0]))
            elif raw_data[0] != '' and raw_data[0] != '\n':
                fieldnames = parse_fieldnames(raw_data[0])
                pattern_02 = ''
                for field in fieldnames:
                    pattern_02 += field+"|"

            txt = re.search('stm[0-9]+',self._txt_logfilepath)
            intf = self._txt_logfilepath[txt.start():txt.end()]
###############################################################################
# CASE when source_host exist in CMD
###############################################################################
            if re.search('source_host=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd):
                src = re.search('source_host=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd)
                src_host = self._cmd[src.start()+12:src.end()]
                inf = re.search('stm[0-9]+', self._cmd)
                outbound = self._cmd[inf.start():inf.end()]
                if outbound == self.d_interface['internal']:
                    filename_by_src_path = "{}/{}{}/{}_outbound_flows.txt".format(
                        FLOW_USER_LOG_FOLDER, self._foldername[0],
                        self._foldername[1], src_host)
                    #logger_monitor.info("{}/{}{}/{}_outbound_flows.txt".format(
                    #    FLOW_USER_LOG_FOLDER, self._foldername[0],
                    #    self._foldername[1], src_host))
                if not (os.path.isfile(filename_by_src_path)):
                    txt_by_src = open(filename_by_src_path, 'w')
                    txt_by_src.close()
                    proc = subprocess.Popen(self._cmd, shell=True,
                                            stdout=subprocess.PIPE, bufsize=1)
                    with proc.stdout:
                        for line in iter(proc.stdout.readline, b''):
                            if re.search(pattern, line):
                                m = re.search(pattern, line)
                                startidx = m.start()
                                endidx = m.end()
                                flow_time = line[startidx:endidx]
                            line = re.sub(pattern_04, "", line)
                            line = re.sub(pattern_01, "", line)
    #                       line = re.sub(pattern_02, "", line)
                            line = re.sub('"+', "", line)
                            line = re.sub(pattern_03, "", line)
                            # ERR Routine
                            if 'Cannot connect to server' in line:
                                is_extracted = False
                                logger_recorder.error("{} is not extracted! - {}".format(filename_by_src_path, line))
                            elif 'does not exist' in line:
                                is_extracted = False
                                logger_recorder.error("{} is not extracted! - {}".format(filename_by_src_path, line))
                            elif 'no matching objects' in line:
                                is_extracted = False
                                logger_recorder.error("{} is not extracted! - {}".format(filename_by_src_path, line))
                            elif line != '' and line != '\n':
                                is_extracted = True
                                fieldnames.insert(0, 'timestamp')
                                with open(filename_by_src_path, "a") as fh:
                                    if not fieldnames[1] in line:
                                        fh.write('{}\t'.format(flow_time))
                                        fh.write('{}'.format(line))
                                    else:
                                        fh.write('timestamp\t\t')
                                        fh.write('{}'.format(line))
                            else:
                                pass
                    if is_extracted:
                        logger_recorder.info('Flow info host {} extracted to {} successfully!'.format(src_host, filename_by_src_path))
                else:
                    proc = subprocess.Popen(self._cmd, shell=True,
                                            stdout=subprocess.PIPE, bufsize=1)
                    with proc.stdout:
                        for line in iter(proc.stdout.readline, b''):
                            # ER Routine
                            if 'Cannot connect to server' in line:
                                is_extracted = False
                                logger_recorder.error("{} is not extracted! - {}".format(filename_by_src_path, line))
                            elif 'does not exist' in line:
                                is_extracted = False
                                logger_recorder.error("{} is not extracted! - {}".format(filename_by_src_path, line))
                            elif 'no matching objects' in line:
                                is_extracted = False
                                logger_recorder.error("{} is not extracted! - {}".format(filename_by_src_path, line))
                            elif line != '' and line != '\n':
                                is_extracted = True
                                if re.search(pattern, line):
                                    m = re.search(pattern, line)
                                    startidx = m.start()
                                    endidx = m.end()
                                    flow_time = line[startidx:endidx]
                                line = re.sub(pattern_04, "", line)
                                line = re.sub(pattern_01, "", line)
                                line = re.sub(pattern_02, "", line)
                                line = re.sub('"+', "", line)
                                line = re.sub(pattern_03, "", line)
                                #re.sub(pattern_03, "\r\n", csv_data_row)
                                with open(filename_by_src_path, "a") as fh:
                                    fh.write('{}\t'.format(flow_time))
                                    fh.write('{}'.format(line))
                            else:
                                pass
                    if is_extracted:
                        logger_recorder.info('Flow info host {} extracted to {} successfully!'.format(src_host, filename_by_src_path))
###############################################################################
# CASE when dest_host exist in CMD
###############################################################################
            elif re.search('dest_host=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd):
                dst = re.search('dest_host=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd)
                dst_host = self._cmd[dst.start()+10:dst.end()]
                inf = re.search('stm[0-9]+', self._cmd)
                inbound = self._cmd[inf.start():inf.end()]
                if inbound == self.d_interface['external']:
                    filename_by_dst_path = "{}/{}{}/{}_inbound_flows.txt".format(
                        FLOW_USER_LOG_FOLDER, self._foldername[0], self._foldername[1], dst_host)
                if not (os.path.isfile(filename_by_dst_path)):
                    txt_by_dst = open(filename_by_dst_path, 'w')
                    txt_by_dst.close()
                    proc = subprocess.Popen(self._cmd, shell=True,
                                            stdout=subprocess.PIPE, bufsize=1)
                    with proc.stdout:
                        for line in iter(proc.stdout.readline, b''):
                            if re.search(pattern, line):
                                m = re.search(pattern, line)
                                startidx = m.start()
                                endidx = m.end()
                                flow_time = line[startidx:endidx]
                            line = re.sub(pattern_04, "", line)
                            line = re.sub(pattern_01, "", line)
    #                       line = re.sub(pattern_02, "", line)
                            line = re.sub('"+', "", line)
                            line = re.sub(pattern_03, "", line)
                            # ERR Routine
                            if 'Cannot connect to server' in line:
                                is_extracted = False
                                logger_recorder.error("{} is not extracted! - {}".format(filename_by_dst_path, line))
                            elif 'does not exist' in line:
                                is_extracted = False
                                logger_recorder.error("{} is not extracted! - {}".format(filename_by_dst_path, line))
                            elif 'no matching objects' in line:
                                is_extracted = False
                                logger_recorder.error("{} is not extracted! - {}".format(filename_by_dst_path, line))
                            elif line != '' and line != '\n':
                                is_extracted = True
                                fieldnames.insert(0, 'timestamp')
                                with open(filename_by_dst_path, "a") as fh:
                                    if not fieldnames[1] in line:
                                        fh.write('{}\t'.format(flow_time))
                                        fh.write('{}'.format(line))
                                    else:
                                        fh.write('timestamp\t\t')
                                        fh.write('{}'.format(line))
                            else:
                                pass
                    if is_extracted:
                        logger_recorder.info('Flow info host {} extracted to {} successfully!'.format(dst_host, filename_by_dst_path))
                else:
                    proc = subprocess.Popen(self._cmd, shell=True,
                                            stdout=subprocess.PIPE, bufsize=1)
                    with proc.stdout:
                        for line in iter(proc.stdout.readline, b''):
                            # ER Routine
                            if 'Cannot connect to server' in line:
                                is_extracted = False
                                logger_recorder.error("{} is not extracted! - {}".format(filename_by_dst_path, line))
                            elif 'does not exist' in line:
                                is_extracted = False
                                logger_recorder.error("{} is not extracted! - {}".format(filename_by_dst_path, line))
                            elif 'no matching objects' in line:
                                is_extracted = False
                                logger_recorder.error("{} is not extracted! - {}".format(filename_by_dst_path, line))
                                #logger_recorder.error("{} - {}".format(line, self._cmd))
                            elif line != '' and line != '\n':
                                is_extracted = True
                                if re.search(pattern, line):
                                    m = re.search(pattern, line)
                                    startidx = m.start()
                                    endidx = m.end()
                                    flow_time = line[startidx:endidx]
                                line = re.sub(pattern_04, "", line)
                                line = re.sub(pattern_01, "", line)
                                line = re.sub(pattern_02, "", line)
                                line = re.sub('"+', "", line)
                                line = re.sub(pattern_03, "", line)
                                #re.sub(pattern_03, "\r\n", csv_data_row)
                                with open(filename_by_dst_path, "a") as fh:
                                    fh.write('{}\t'.format(flow_time))
                                    fh.write('{}'.format(line))
                            else:
                                pass
                    if is_extracted:
                        logger_recorder.info('Flow info host {} extracted to {} successfully!'.format(dst_host, filename_by_dst_path))
###############################################################################
# CASE when source_host and dest_host don't exist in CMD
###############################################################################
            if not re.search('source_host|dest_host', self._cmd):
                if not (os.path.isfile(self._txt_logfilepath)):
                    create_folder(self._foldername)
                    txt_file = open(self._txt_logfilepath, 'w')
                    txt_file.close()
                    proc = subprocess.Popen(self._cmd, shell=True,
                                            stdout=subprocess.PIPE, bufsize=1)
                    with proc.stdout:
                        for line in iter(proc.stdout.readline, b''):
                            if re.search(pattern, line):
                                m = re.search(pattern, line)
                                startidx = m.start()
                                endidx = m.end()
                                flow_time = line[startidx:endidx]
                            line = re.sub(pattern_04, "", line)
                            line = re.sub(pattern_01, "", line)
    #                        line = re.sub(pattern_02, "", line)
                            line = re.sub('"+', "", line)
                            line = re.sub(pattern_03, "", line)
                            # ERR Routine
                            if 'Cannot connect to server' in line:
                                is_extracted = False
                                #logger_recorder.error("{} - {}".format(line, self._cmd))
                            elif 'does not exist' in line:
                                is_extracted = False
                                #logger_recorder.error("{} - {}".format(line, self._cmd))
                            elif 'no matching objects' in line:
                                is_extracted = False
                                #logger_recorder.error("{} - {}".format(line, self._cmd))
                            elif line != '' and line != '\n':
                                is_extracted = True
                                fieldnames.insert(0, 'timestamp')
                                with open(self._txt_logfilepath, "a") as fh:
                                    if not fieldnames[1] in line:
                                        fh.write('{}\t'.format(flow_time))
                                        fh.write('{}'.format(line))
                                    else:
                                        fh.write('timestamp\t\t')
                                        fh.write('{}'.format(line))
                            else:
                                pass
                    if is_extracted:
                        logger_recorder.info('Flow info default from interfaces {} is extracted to {} \
successfully!'.format(intf, self._txt_logfilepath))
                else:
                    proc = subprocess.Popen(self._cmd, shell=True,
                                           stdout=subprocess.PIPE, bufsize=1)
                    with proc.stdout:
                        for line in iter(proc.stdout.readline, b''):
                            # ER Routine
                            if 'Cannot connect to server' in line:
                                is_extracted = False
                                #logger_recorder.error("{} - {}".format(line, self._cmd))
                            elif 'does not exist' in line:
                                is_extracted = False
                                #logger_recorder.error("{} - {}".format(line, self._cmd))
                            elif 'no matching objects' in line:
                                is_extracted = False
                                #logger_recorder.error("{} - {}".format(line, self._cmd))
                            elif line != '' and line != '\n':
                                is_extracted = True
                                if re.search(pattern, line):
                                    m = re.search(pattern, line)
                                    startidx = m.start()
                                    endidx = m.end()
                                    flow_time = line[startidx:endidx]
                                    line = re.sub(pattern_04, "", line)
                                line = re.sub(pattern_01, "", line)
                                line = re.sub(pattern_02, "", line)
                                line = re.sub('"+', "", line)
                                line = re.sub(pattern_03, "", line)
                                #re.sub(pattern_03, "\r\n", csv_data_row)
                                with open(self._txt_logfilepath, "a") as fh:
                                    fh.write('{}\t'.format(flow_time))
                                    fh.write('{}'.format(line))
                            else:
                                pass
                    if is_extracted:
                        logger_recorder.info('Flow info default from interfaces {} is extracted to {} \
successfully!'.format(intf, self._txt_logfilepath))
        except Exception as e:
            logger_recorder.error("start_fr_txt() cannot be executed, {}".format(e))
            pass

################################################################################
#       Write the csv type log from the command
################################################################################
    def start_fr_csv(self):
        try:
            csv = re.search('stm[0-9]+',self._csv_logfilepath)
            intf = self._csv_logfilepath[csv.start():csv.end()]
###############################################################################
# CASE when source_host exist in CMD
###############################################################################
            if re.search('source_host=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd):
                src = re.search('source_host=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd)
                src_host = self._cmd[src.start()+12:src.end()]
                inf = re.search('stm[0-9]+', self._cmd)
                outbound = self._cmd[inf.start():inf.end()]
                if outbound == self.d_interface['internal']:
                    filename_by_src_path = "{}/{}{}/{}_outbound_flows.csv".format(
                        FLOW_USER_LOG_FOLDER, self._foldername[0],
                        self._foldername[1], src_host)
                    #logger_monitor.info("{}/{}{}/{}_outbound_flows.txt".format(
                    #    FLOW_USER_LOG_FOLDER, self._foldername[0],
                    #    self._foldername[1], src_host))
                if not (os.path.isfile(filename_by_src_path)):
                    create_folder(self._foldername)
                    csv_by_src = open(filename_by_src_path, 'w')
                    csv_by_src.close()
                    raw_data = subprocess_open(self._cmd)
                    if 'Cannot connect to server' in raw_data[0]:
                        logger_recorder.error("{} is not extracted! - {}".format(filename_by_src_path, raw_data[0]))
                    elif 'does not exist' in raw_data[0]:
                        logger_recorder.error("{} is not extracted! - {}".format(filename_by_src_path, raw_data[0]))
                    elif 'no matching objects' in raw_data[0]:
                        logger_recorder.error("{} is not extracted! - {}".format(filename_by_src_path, raw_data[0]))
                    else:
                        fieldnames = parse_fieldnames(raw_data[0])
                        fieldnames.insert(0, 'timestamp')
                        with open(filename_by_src_path, "a") as fh:
                            fh.write(','.join(fieldnames))
                            fh.write('\r\n')
                        time.sleep(1)
                        self.parse_fr_csv(raw_data[0], filename_by_src_path, src_host)
                        #logger_recorder.info('Flow info host {} extracted to {} successfully!'.format(src_host, filename_by_src_path))
                else:
                    raw_data = subprocess_open(self._cmd)
                    if 'Cannot connect to server' in raw_data[0]:
                        logger_recorder.error("{} is not extracted! - {}".format(filename_by_src_path, raw_data[0]))
                    elif 'does not exist' in raw_data[0]:
                        logger_recorder.error("{} is not extracted! - {}".format(filename_by_src_path, raw_data[0]))
                    elif 'no matching objects' in raw_data[0]:
                        logger_recorder.error("{} is not extracted! - {}".format(filename_by_src_path, raw_data[0]))
                    else:
                        self.parse_fr_csv(raw_data[0], filename_by_src_path, src_host)
                        #logger_recorder.info('Flow info host {} extracted to {} successfully!'.format(src_host, filename_by_src_path))
###############################################################################
# CASE when dest_host exist in CMD
###############################################################################
            elif re.search('dest_host=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd):
                dst = re.search('dest_host=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', self._cmd)
                dst_host = self._cmd[dst.start()+10:dst.end()]
                inf = re.search('stm[0-9]+', self._cmd)
                inbound = self._cmd[inf.start():inf.end()]
                if inbound == self.d_interface['external']:
                    filename_by_dst_path = "{}/{}{}/{}_inbound_flows.csv".format(
                        FLOW_USER_LOG_FOLDER, self._foldername[0], self._foldername[1], dst_host)
                if not (os.path.isfile(filename_by_dst_path)):
                    create_folder(self._foldername)
                    csv_by_dst = open(filename_by_dst_path, 'w')
                    csv_by_dst.close()
                    raw_data = subprocess_open(self._cmd)
                    if 'Cannot connect to server' in raw_data[0]:
                        logger_recorder.error("{} is not extracted! - {}".format(filename_by_dst_path, raw_data[0]))
                    elif 'does not exist' in raw_data[0]:
                        logger_recorder.error("{} is not extracted! - {}".format(filename_by_dst_path, raw_data[0]))
                    elif 'no matching objects' in raw_data[0]:
                        logger_recorder.error("{} is not extracted! - {}".format(filename_by_dst_path, raw_data[0]))
                    else:
                        fieldnames = parse_fieldnames(raw_data[0])
                        fieldnames.insert(0, 'timestamp')
                        with open(filename_by_dst_path, "a") as fh:
                            fh.write(','.join(fieldnames))
                            fh.write('\r\n')
                        time.sleep(1)
                        self.parse_fr_csv(raw_data[0], filename_by_dst_path, dst_host)
                        #logger_recorder.info('Flow info host {} extracted to {} successfully!'.format(dst_host, filename_by_dst_path))
                else:
                    raw_data = subprocess_open(self._cmd)
                    if 'Cannot connect to server' in raw_data[0]:
                        logger_recorder.error("{} is not extracted! - {}".format(filename_by_dst_path, raw_data[0]))
                    elif 'does not exist' in raw_data[0]:
                        logger_recorder.error("{} is not extracted! - {}".format(filename_by_dst_path, raw_data[0]))
                    elif 'no matching objects' in raw_data[0]:
                        logger_recorder.error("{} is not extracted! - {}".format(filename_by_dst_path, raw_data[0]))
                    else:
                        self.parse_fr_csv(raw_data[0], filename_by_dst_path, dst_host)
                        #logger_recorder.info('Flow info host {} extracted to {} successfully!'.format(dst_host, filename_by_dst_path))
###############################################################################
# CASE when source_host and dest_host don't exist in CMD
###############################################################################
            if not re.search('source_host|dest_host', self._cmd):
                if not (os.path.isfile(self._csv_logfilepath)):
                    create_folder(self._foldername)
                    csv_file = open(self._csv_logfilepath, 'w')
                    csv_file.close()
                    raw_data = subprocess_open(self._cmd)
                    if 'Cannot connect to server' in raw_data[0]:
                        logger_recorder.error("{} - {}".format(self._cmd, raw_data[0]))
                    elif 'does not exist' in raw_data[0]:
                        logger_recorder.error("{} - {}".format(self._cmd, raw_data[0]))
                    elif 'no matching objects' in raw_data[0]:
                        logger_recorder.error("{} - {}".format(self._cmd, raw_data[0]))
#                        logger_recorder.error("{} - {}".format(raw_data[0], self._cmd))
                    else:
                        fieldnames = parse_fieldnames(raw_data[0])
                        fieldnames.insert(0, 'timestamp')
                        with open(self._csv_logfilepath, "a") as fh:
                            fh.write(','.join(fieldnames))
                            fh.write('\r\n')
                        time.sleep(1)
                        self.parse_fr_csv(raw_data[0], self._csv_logfilepath, intf)
                        #logger_recorder.info('Flow info default from interfaces {} is extracted to {} successfully!'.format(intf, self._csv_logfilepath))
                else:
                    raw_data = subprocess_open(self._cmd)
                    if 'Cannot connect to server' in raw_data[0]:
                        logger_recorder.error("{} - {}".format(self._cmd, raw_data[0]))
                    elif 'does not exist' in raw_data[0]:
                        logger_recorder.error("{} - {}".format(self._cmd, raw_data[0]))
                    elif 'no matching objects' in raw_data[0]:
                        logger_recorder.error("{} - {}".format(self._cmd, raw_data[0]))
                    else:
                        self.parse_fr_csv(raw_data[0], self._csv_logfilepath, intf)
                        #logger_recorder.info('Flow info default from interfaces {} is extracted to {} successfully!'.format(intf, self._csv_logfilepath))
        except Exception as e:
            logger_recorder.error("start_fr_csv() cannot be executed, {}".format(e))
            pass

################################################################################
#       Parse the data into csv data
#       csv_data_row : data from command
#       logpath : path to write data
################################################################################
    def parse_fr_csv(self, csv_data_row, logpath, *args):
        try:
            #pattern = r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
            #pattern_01 = r'[-]{1,10}'
            #pattern_03 = r'\s+\n'
            m = re.search(pattern, csv_data_row)
            startidx = m.start()
            endidx = m.end()
            flow_time = csv_data_row[startidx:endidx]

            # Get fieldnames
            fieldnames = parse_fieldnames(csv_data_row)
            # Make field pattern
            pattern_02 = ''
            for field in fieldnames:
                pattern_02 += field+"|"

            csv_data_row = re.sub(pattern, "", csv_data_row)
            csv_data_row = re.sub(pattern_01, "", csv_data_row)
            csv_data_row = re.sub(pattern_02, "", csv_data_row)
            csv_data_row = re.sub(pattern_03, "\r\n", csv_data_row)
            reader = csv.DictReader(itertools.islice(csv_data_row.splitlines(),
                                                     2, None),
                                    delimiter=' ',
                                    skipinitialspace=True,
                                    fieldnames=fieldnames)
            for row in reader:
                with open(logpath, "a") as fh:
                    fh.write('{},'.format(flow_time))
                    writer = csv.DictWriter(f=fh, fieldnames=reader.fieldnames)
                    writer.writerow(row)
        except Exception as e:
            logger_recorder.error("parse_fr_csv() cannot be executed, {}".format(e))
            pass
        else:
            if re.search('[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', args[0]):
                logger_recorder.info('Flow info host {} extracted to {} successfully!'.format(src_host, filename_by_src_path))
            else:
                logger_recorder.info('Flow info default from interfaces {} is extracted to {} successfully!'.format(args[0], logpath))

