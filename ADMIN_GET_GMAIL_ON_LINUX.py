#!/usr/bin/env python
# coding: utf-8



# Importing required libraries
import time
import logging
import os
import socket
import base64
import re
import dateutil.parser as parser
import datetime
import csv
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import configparser
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


GmailSCOPES = None             # API
SERVICE_ACCOUNT_EMAIL = None   # The email of the service account
PK12_FILE = None               # The secret key
PK12_PASSWORD = None           # password
maxResults = None              # Get the latest mail number
GmailIdPath = None             # Staff email
HaveGmailIdPath = None         # Existing mailbox
GmailPath = None               # Save the mail
GmailNum = None
AdminDirectoryAPITokenFile = None
GmailAdministratorFile = None
AdminDirectoryAPISCOPES = None
AbnormalGmailAddress = None
NormalGmailAddress = None
NormalGmailFileName = None
AbnormalGmailFileName = None

socket.setdefaulttimeout(300)

# get gmail List
def getgmailList():
    gmailList = []
    creds = None
    if os.path.exists(AdminDirectoryAPITokenFile):
        with open(AdminDirectoryAPITokenFile, 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(GmailAdministratorFile, AdminDirectoryAPISCOPES)
            creds = flow.run_local_server()
        with open(AdminDirectoryAPITokenFile, 'wb') as token:
            pickle.dump(creds, token)
    service = build('admin', 'directory_v1', credentials=creds)
    results = service.users().list(customer='my_customer',maxResults=500,orderBy='email').execute()
    users = results.get('users', [])
    nextPageToken = results.get('nextPageToken', {})
    if not users:
        logger.error("No users in the domain.")
    else:
        for user in users:
            gmailList.append(user['primaryEmail'])
            # print(u'{0} {1}'.format(user['primaryEmail'],user['name']['fullName']))


    loopFlag = True
    while loopFlag: #Loop every page's data for gmail address
        if nextPageToken:
            results = service.users().list(customer='my_customer', pageToken = nextPageToken, maxResults=500, orderBy='email').execute()
            users = results.get('users', [])
            if not users:
                logger.error("No users in the domain.")
            else:
                for user in users:
                    gmailList.append(user['primaryEmail'])
                    # print(u'{0} {1}'.format(user['primaryEmail'],user['name']['fullName']))

            nextPageToken = results.get('nextPageToken', {})
            if not nextPageToken:
                loopFlag = False
                break

    return gmailList


def write_log():
    '''
    写log
    :return: 返回logger对象
    '''
    # 获取logger实例，如果参数为空则返回root logger
    logger = logging.getLogger()
    now_date = datetime.datetime.now().strftime('%Y%m%d')
    log_file = now_date + ".log"  # 文件日志

    # if not os.path.exists(os.path.join(os.path.dirname(__file__))+os.sep+'log'):  # python文件同级别创建log文件夹
    #     os.makedirs(os.path.join(os.path.dirname(__file__))+os.sep+'log')

    if not os.path.exists(r'/home/creat/SYS/TORASINNYOU/GSUITEGETGMAILDATA/log'):  # python文件同级别创建log文件夹
        os.makedirs(r'/home/creat/SYS/TORASINNYOU/GSUITEGETGMAILDATA/log')
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)s line:%(lineno)s %(message)s')
    file_handler = logging.FileHandler(r'/home/creat/SYS/TORASINNYOU/GSUITEGETGMAILDATA/log' + os.sep + log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
    # 为logger添加的日志处理器，可以自定义日志处理器让其输出到其他地方
    logger.addHandler(file_handler)
    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.INFO)
    return logger


def removeBlank(MyString):
    '''
    :param MyString: 要替换空白的字符串
    :return: 去掉空白后的字符串
    '''
    try:
        MyString = re.sub('[\s+]', '', str(MyString)).replace('\n','-t')
        if (not MyString) or MyString == 'null':
            MyString = '_'
        return MyString
    except Exception as ex:
        logger.error("Call method removeBlank() error!")
        raise ex


# def getGmailList():
#     '''
#     读人员邮箱地址
#     '''
#     try:
#         txt_config_list = []
#
#         # print(GmailIdPath)
#         with open(GmailIdPath, 'r', encoding='utf-8') as txtConfig:
#             lines = txtConfig.readlines()
#             for line in lines:
#                 line = line.strip()
#                 if not line:  # 如果line是空
#                     continue
#                 else:
#                     txt_config_list.append(line)
#             return txt_config_list
#     except Exception as ex:
#         logger.error("Call method getcalendarIdList() error!")
#         raise ex


def getgmail(gmailList):
    '''
    :param GmailList: API中GmailId对应的List
    :return: 要保存成文本的List

    从根节点到分支节点(叶子节点)的全部元素的说明,空值用“_”代替
    共16个元素:
    邮箱地址      gmailid
    邮件id        id                  "id": "16859c3619738d4d",
    历史邮件id    threadid            "threadId": "16859c3619738d4d",
    状态标签      labelids            "labelIds": ["IMPORTANT","CATEGORY_PERSONAL","INBOX"],
    内容片断      snippet             "snippet": "好天气啊！",
    历史信息id    historyid           "historyId": "15135",
    时间戳        internaldate        "internalDate": "1547694199000",
    送信者        from                "name": "From","value": "\"宋家军\" \u003cdapsjj@qq.com\u003e"
    受件者        to                  "name": "To","value": "10113982songjiajun \u003c10113982songjiajun@gmail.com\u003e"
    抄送人        cc                  "name": "Cc","value": "\"宋家军\" \u003cdapsjj@qq.com\u003e"
    密送人        bcc                 "value": "\"宋家军\" \u003cdapsjj@qq.com\u003e, 103762438@qq.com"
    标题          subject             "name": "Subject","value": "天气情况测试"
    时间          date                "name": "Date","value": "Thu, 17 Jan 2019 11:03:19 +0800"
    附件名称      filename            "partId": "1"，"filename": "外部回复.txt"   "partId": "2","filename": "454.txt",
    邮件内容      content
    '''

    try:
        # Create credentials
        # Get Gmail information
        final_list = []
        gmail_haveid = []
        gailnum = []
        AbnormalGmailAddressList = []
        NormalGmailAddressList = []
        for user_id in gmailList:
            # print(user_id)

            num_unread = 0  # 未读
            num_inbox = 0  # 邮箱
            draft = 0  # 草稿
            chat = 0  # 聊天

            try:
                # Getting all the unread messages from Inbox
                # labelIds can be changed accordingly
                credentials = ServiceAccountCredentials.from_p12_keyfile(SERVICE_ACCOUNT_EMAIL, PK12_FILE,
                                                                         PK12_PASSWORD, scopes=GmailSCOPES)
                credentials = credentials.create_delegated(user_id)
                bryan = build('gmail', 'v1', credentials=credentials)
                unread_msgs = bryan.users().messages().list(userId=user_id, maxResults=maxResults).execute()  # , maxResults=1
                # We get a dictonary. Now reading values for the key 'messages'
                mssg_list = unread_msgs.get('messages',{})
                NormalGmailAddressList.append(user_id)
            except Exception as e:
                logger.info('GmailId:' + user_id + 'has error')
                AbnormalGmailAddressList.append(user_id)
                continue

            # print("Total unread messages in inbox: ", str(len(mssg_list)))
            for mid in mssg_list:
                m_id = mid.get('id','')  # '16859c3619738d4d'
                try:
                    message = bryan.users().messages().get(userId=user_id, id=m_id).execute()  # fetch the message using API
                except Exception as e:
                    logger.info('GmailId:' + user_id + 'has error')
                    AbnormalGmailAddressList.append(user_id)
                    continue
                temp_dict = {}
                temp_dict['gmailid'] = removeBlank(user_id)  # xx@xx
                # temp_dict = dict(temp_dict,message[["id","threadId","labelIds","snippet","historyId","internalDate"]])
                temp_dict['id'] = removeBlank(message.get("id",''))
                temp_dict['threadId'] = removeBlank(message.get("threadId",''))
                temp_dict['labelIds'] = removeBlank(message.get("labelIds",''))
                for lab in list(message.get("labelIds",'')):
                    if lab == 'DRAFT':
                        draft += 1
                        break
                    elif lab == 'CHAT':
                        chat += 1
                        break
                    elif lab == 'UNREAD':
                        num_unread += 1
                        break

                temp_dict['snippet'] = removeBlank(message.get("snippet",''))
                temp_dict['historyId'] = removeBlank(message.get("historyId",''))
                temp_dict['internalDate'] = removeBlank(message.get("internalDate",''))

                payld = message.get("payload",{})
                headr = payld.get('headers',{})
                for one in headr:  # getting the Subject
                    if one.get('name','') == "From":
                        temp_dict["from"] = removeBlank(one.get('value',''))
                    elif one.get('name','') == "To":
                        temp_dict["to"] = removeBlank(one.get('value',''))
                    elif one.get('name','') == 'Cc':
                        temp_dict['cc'] = removeBlank(one.get('value',''))
                    elif one.get('name','') == 'Bcc':
                        temp_dict['bcc'] = removeBlank(one.get('value',''))
                    elif one.get('name','') == 'Subject':
                        temp_dict['subject'] = removeBlank(one.get('value',''))
                    elif one.get('name','') == 'Date':
                        msg_date = one.get('value','')
                        try:
                            date_parse = parser.parse(msg_date)
                            m_date = date_parse.date()
                        except Exception as ex:
                            m_date='_'
                        temp_dict['Date'] = removeBlank(m_date)

                    parts = payld.get('parts',{})
                    body = payld.get('body',{})
                    if parts:
                        filename = ''
                        for par in parts:
                            filename = filename + par.get('filename','') + ','
                        filename = filename.strip(',')
                        if not filename.strip(','):
                            filename = '_'
                        temp_dict['filename'] = filename

                        # content
                        if parts[0].get('parts'):
                            content = parts[0]['parts'][0].get('body',{}).get('data','')
                        else:
                            content = parts[0].get('body',{}).get('data','')
                        if content:
                            content = content.replace("-", "+")
                            content = content.replace("_", "/")
                            content = base64.b64decode(bytes(content, 'UTF-8'))
                            content = str(content, 'utf-8')
                            temp_dict['content'] = removeBlank(content)
                    elif body:
                        content = body.get('data','')
                        if content:
                            content = content.replace("-", "+")
                            content = content.replace("_", "/")
                            content = base64.b64decode(bytes(content, 'UTF-8'))
                            content = str(content, 'utf-8')
                            temp_dict['content'] = removeBlank(content)
                final_list.append(temp_dict)

                gmail_haveid.append(m_id)

            # Read unread count
            num_inbox = len(mssg_list) - draft - chat - num_unread
            gailnum.append({'user_id': user_id, 'num_unread': num_unread, 'num_inbox': num_inbox, 'draft': draft, 'chat': chat})

        # 保存异常人员的邮箱
        now_date = datetime.datetime.now().strftime('%Y%m%d')
        if not os.path.exists(AbnormalGmailAddress + now_date):  # python文件同级别创建log文件夹
            os.makedirs(AbnormalGmailAddress + now_date)
        with open(AbnormalGmailAddress + now_date + os.sep + AbnormalGmailFileName, "w", encoding="utf-8") as fo:
            fo.write('\n'.join([i for i in AbnormalGmailAddressList]))

        # 保存正常人员的邮箱
        if not os.path.exists(NormalGmailAddress + now_date):  # python文件同级别创建log文件夹
            os.makedirs(NormalGmailAddress + now_date)
        with open(NormalGmailAddress + now_date + os.sep + NormalGmailFileName, "w", encoding="utf-8") as fo:
            fo.write('\n'.join([i for i in NormalGmailAddressList]))
        return final_list, gailnum
    except Exception as ex:
        logger.error("Call method getgmail() error!")
        raise ex


def write_data(final_list):
    try:

        with open(GmailPath, 'w', encoding='utf-8', newline='') as csvfile:
            fieldnames = ['gmailid', "id", "threadId", "labelIds", "snippet", "historyId", "internalDate", "from",
                          "to", 'cc', 'bcc', 'subject', 'Date', 'filename', 'content']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=' ', restval='_')
            # writer.writeheader()
            for val in final_list:
                writer.writerow(val)
    except Exception as ex:
        logger.error("Call method write_data() error!")
        raise ex


def write_num(gailnumlist):
    try:
        with open(GmailNum, 'w', encoding='utf-8', newline='') as numfile:
            fieldnum = ['user_id', 'num_unread', 'num_inbox', 'draft', 'chat']
            writer = csv.DictWriter(numfile, fieldnames=fieldnum, delimiter=' ', restval='_')
            # writer.writeheader()
            for val in gailnumlist:
                writer.writerow(val)
    except Exception as ex:
        logger.error("Call method write_data() error!")
        raise ex

def readConfig():
    '''
    读dateConfig.ini,获取事件的开始时间、事件的结束时间、创建或者更新时间在多少天内的数据(最多29天)、要保存的文件夹的路径
    '''
    global GmailSCOPES
    global SERVICE_ACCOUNT_EMAIL
    global PK12_FILE
    global PK12_PASSWORD
    global maxResults
    global GmailIdPath
    global HaveGmailIdPath
    global GmailPath
    global AdminDirectoryAPITokenFile
    global GmailAdministratorFile
    global AdminDirectoryAPISCOPES
    global AbnormalGmailAddress
    global NormalGmailAddress
    global NormalGmailFileName
    global AbnormalGmailFileName
    global GmailNum
    if os.path.exists("/TORASINNYOU/TBL/GSUITE/GMAIL/GmailConfig.ini"):
        try:
            conf = configparser.ConfigParser()
            conf.read("/TORASINNYOU/TBL/GSUITE/GMAIL/GmailConfig.ini", encoding="utf-8-sig")
            GmailSCOPES = conf.get("GmailAPI", "GmailAPI")
            SERVICE_ACCOUNT_EMAIL = conf.get("Credentials", "SERVICE_ACCOUNT_EMAIL")
            PK12_FILE = conf.get("Credentials", "PK12_FILE")
            PK12_PASSWORD = conf.get("Credentials", "PK12_PASSWORD")
            maxResults = conf.get("maxResults", "maxResults")
            # GmailIdPath = conf.get("GmailIdPath", "GmailIdPath")
            GmailPath = conf.get("GmailPath", "GmailPath")
            GmailNum = conf.get("GMAILNUM", "GMAILNUM")
            AdminDirectoryAPITokenFile = conf.get("AdminDirectoryAPITokenFile", "AdminDirectoryAPITokenFile")
            GmailAdministratorFile = conf.get("GmailAdministratorFile", "GmailAdministratorFile")
            AdminDirectoryAPISCOPES = conf.get("AdminDirectoryAPISCOPES", "AdminDirectoryAPISCOPES")
            NormalGmailAddress = conf.get("NormalGmailAddress", "NormalGmailAddress")
            AbnormalGmailAddress = conf.get("AbnormalGmailAddress", "AbnormalGmailAddress")
            NormalGmailFileName = conf.get("NormalGmailFileName", "NormalGmailFileName")
            AbnormalGmailFileName = conf.get("AbnormalGmailFileName", "AbnormalGmailFileName")
        except Exception as ex:
            logger.error("Content in dateConfig.ini has error.")
            logger.error("Exception:" + str(ex))
            raise ex
    else:
        logger.error("DateConfig.ini doesn't exist!")


if __name__ == '__main__':
    logger = write_log()  # Get log

    time_start = datetime.datetime.now()
    start = time.time()
    logger.info("Program start,now time is:" + str(time_start))
    readConfig()
    gmailList = getgmailList()
    final_list,gailnumlist = getgmail(gmailList)
    df = write_data(final_list)
    numdf = write_num(gailnumlist)
    time_end = datetime.datetime.now()
    end = time.time()
    logger.info("Program end,now time is:" + str(time_end))
    logger.info("Program run : %f seconds" % (end - start))
    # print('程序结束')
