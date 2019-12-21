import pymongo
from pymongo import MongoClient
import os
import json
import time

### 匯入company data, 增加{source:104/1111/518}
def import_company():
    formatkey = [
        '公司網址',
        '公司名稱',
        '產業類型',
        '產業描述',
        '資本額',
        '員工人數',
        '負責人',
        '聯絡人',
        '電話',
        '傳真',
        '地址',
        '官網連結',
        '公司介紹',
        '主要商品',
        '福利制度',
        '統一編號',
        '成立時間']
    mapping = {
        '行業別': '產業類型',
        '行業說明': '產業描述',
        '公司電話': '電話',
        '公司傳真': '傳真',
        '聯絡地址': '地址',
        '網站位址': '官網連結',
        '產品服務': '主要商品',
        '工作福利': '福利制度'
    }

    db = MongoClient('localhost', 27017).club
    db.company.remove({'source': '1111'})
    #db.company_1111.drop()

    tStart = time.time()  # 計時開始
    start = time.strftime("m-%d %H:%M:%S")
    cnt = 0

    mydir = r'C:\DB103\caseClub\crawler_data\1111_corps'
    entries = os.listdir(mydir)
    for entry in entries:
        jsonfile = mydir + r'\\' + entry
        print(jsonfile, end=' ')
        if os.path.isfile(jsonfile):
            with open(jsonfile) as f:
                orgdata = json.load(f)
                #print(len(orgdata), orgdata)
                newdata = orgdata.copy()
                for key in orgdata:
                    for key1 in mapping:
                        if key == key1:
                            if key == '工作福利':
                                welfare = "".join(orgdata['工作福利'].values())
                                newdata[mapping.get(key1)] = welfare
                                del newdata['工作福利']
                            else:
                                newdata[mapping.get(key1)] = newdata.pop(key)
                    # 去除怪異key-value (相關連結抓取有誤)
                    if key not in formatkey:
                        try:
                            del newdata[key]
                        except:
                            pass

                #去除\n、\xa0、\u3000
                for key in newdata:
                    newdata[key] = "".join(newdata[key].split())
                newdata['source'] = '1111'
                db.company.insert(newdata)
                #db.company_1111.insert(newdata)
                cnt += 1
                # print(len(newdata), newdata)
        else:
            print('not file')
        print()

    print('Done!!', cnt)
    tEnd = time.time()  # 計時結束
    end = time.strftime("m-%d %H:%M:%S")
    print('done!! 花費:', (tEnd - tStart), 'sec')
    print(start, ' ~ ', end)


def import_jobs():
    db = MongoClient('localhost', 27017).club
    db.jobs_1111.drop()

    mydir = r'C:\DB103\caseClub\crawler_data\1111_jobs'
    entries = os.listdir(mydir)
    for entry in entries:
        subdir = mydir + r'\\' + entry
        for e in os.listdir(subdir):
            jsonfile = subdir + r'\\' + e
            print(jsonfile, end=' ')
            if os.path.isfile(jsonfile):
                with open(jsonfile) as f:
                    data = json.load(f)
                    for d in data:
                        d['source'] = '1111'
                        db.jobs_1111.insert(d)
            else:
                print('not file')
            print()

    MongoClient('localhost', 27017).close()


if __name__ == "__main__":
    import_company()
    import_jobs()