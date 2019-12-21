import pymongo
from pymongo import MongoClient
import os
import json
import time

### 匯入company data, 增加{source:104/1111/518}
def import_company(db):
    tStart = time.time()#計時開始
    start = time.strftime("m-%d %H:%M:%S")

    db.company_104.drop()
    # 以104為base
    mydir = r'E:\專題\crawler_data\104_company_json'
    entries = os.listdir(mydir)
    for entry in entries:
        jsonfile = mydir + r'\\' + entry
        print(jsonfile, end=' ')
        if os.path.isfile(jsonfile):
            with open(jsonfile) as f:
                data = json.load(f)
                data['source'] = '104'
                db.company_104.insert(data)
        else:
            print('not file')
        print()

    tEnd = time.time()  # 計時結束
    end = time.strftime("m-%d %H:%M:%S")
    print('done!! 104 company 花費:', (tEnd - tStart), 'sec', (tEnd - tStart)//60, 'min')
    #print(start, ' ~ ', end)

### 匯入jobs data, 增加{source:104/1111/518}
def import_jobs(db):
    tStart = time.time()#計時開始
    start = time.strftime("m-%d %H:%M:%S")

    db.jobs_104.drop()
    # 以104為base
    mydir = r'E:\專題\crawler_data\104_job_json'
    entries = os.listdir(mydir)
    for entry in entries:
        jsonfile = mydir + r'\\' + entry
        print(jsonfile, end=' ')
        if os.path.isfile(jsonfile):
            with open(jsonfile) as f:
                data = json.load(f)
                data['source'] = '104'
                db.jobs_104.insert(data)
        else:
            print('not file')
        print()

    tEnd = time.time()  # 計時結束
    end = time.strftime("m-%d %H:%M:%S")
    print('done!! 104 job 花費:', (tEnd - tStart), 'sec', (tEnd - tStart)//60, 'min')
    #print(start, ' ~ ', end)

if __name__ == "__main__":
    client = MongoClient('localhost', 27017)
    db = client.club
    #db = MongoClient('10.120.28.50', 27017).club

    # 以104格式為主, 僅增加source欄位
    import_company(db)
    import_jobs(db)

    client.close()