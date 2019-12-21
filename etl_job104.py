from pymongo import MongoClient
import pymysql
import time
import pandas as pd

# connect mysql db
from cr_1111.myConnect import myConnect

# 管理責任
def etl_manage(manage):
    global sqlval
    if manage:
        if ("不需" in manage) or ("不須" in manage) or ("未定" in manage):
            sqlval['management'] = "No"
        else:
            sqlval['management'] = "Yes"
            doc = manage.replace('管理', '').replace('人', '')
            mm = doc.split('-')
            if len(mm) > 1:
                sqlval['manage_min'] = int(mm[0])
                sqlval['manage_max'] = int(mm[1])
            else:
                if '以上' in mm[0]:
                    sqlval['manage_min'] = int(mm[0].split('以上')[0])
                    #sqlval['manage_max'] = 999
                elif '以下' in mm[0]:
                    sqlval['manage_min'] = 1
                    sqlval['manage_max'] = int(mm[0].split('以下')[0])
    else:
        sqlval['management'] = "No"


# 工作時間
def etl_worktime(d):
    global sqlval
    if not d:
        return
    sqlval['work_time_raw'] = d
    d = d.replace('上班', '')
    d = d.replace('下班', '')
    d = d.replace('加班', '')
    d = d.replace('常日班', '')
    d = d.replace('白天班', '')
    d = "".join(d.split())
    if d.count('班') > 1 or '輪班' in d:
        sqlval['work_time'] = '輪班'
    else:
        if '日班' in d:
            sqlval['work_time'] = '日班'
        elif '大夜班' in d:
            sqlval['work_time'] = '大夜班'
        elif '晚班' in d or '夜班' in d:
            sqlval['work_time'] = '晚班'
        else:
            whour = int(d.split('~')[0].split(':')[0])
            if whour >= 5 and whour <= 12:
                sqlval['work_time'] = '日班'
            elif whour >= 13 and whour <= 19:
                sqlval['work_time'] = '晚班'
            else:
                sqlval['work_time'] = '大夜班'

# 職務類別
def etl_category(str):
    global sqlval, cg104, df_errlog
    if not str:
        return
    sqlval['category_raw'] = str
    match = False
    sp = str.split(',')
    for d in sp:
        d = d.strip()
        if cg104.get(d):
            match = True
            sqlval['category_top'] = cg104.get(d).get('L')
            sqlval['category_mid'] = cg104.get(d).get('M')
            sqlval['category_small'] = d
            break
    if not match:
        print('職務類別match Failed', str)
        errlog = {
            "ERR": "職務類別match Failed",
            "SQL": sqlval['job_link'],
            "VALUES": str
        }
        df_errlog = df_errlog.append(errlog, ignore_index=True)


# 薪資
def etl_salary(doc):
    global sqlval
    if '月薪' in doc:
        salary = doc.replace(',', '').split('~')
        if len(salary) > 1:
            sqlval['salary_min'] = salary[0].split(' ')[1]
            sqlval['salary_max'] = salary[1].split('元')[0]
        else:
            min = salary[0].split(' ')[1]
            min = min.replace('元', '').replace('以上', '')
            sqlval['salary_min'] = min
            sqlval['salary_max'] = min
    elif '年薪' in doc:
        salary = doc.replace(',', '').split('~')
        if len(salary) > 1:
            sqlval['salary_min'] = int(salary[0].split(' ')[1]) // 12
            sqlval['salary_max'] = int(salary[1].split('元')[0]) // 12
        else:
            min = salary[0].split(' ')[1]
            min = min.replace('元', '').replace('以上', '')
            min = int(min) // 12
            sqlval['salary_min'] = min
            sqlval['salary_max'] = min


# 需求人數
def etl_quantity(doc):
    global sqlval
    if not doc:
        return
    quantity = doc.replace('人', '').strip().split('至')
    if len(quantity) > 1:
        sqlval['quantity_min'] = int(quantity[0])
        sqlval['quantity_max'] = int(quantity[1])
    elif quantity[0] != '不限':
        sqlval['quantity_min'] = int(quantity[0])
        sqlval['quantity_max'] = int(quantity[0])


# 學歷要求
def etl_education(doc):
    global sqlval
    if not doc:
        return
    sqlval['accept_education'] = doc
    iden = doc.split('、')
    iLen = len(iden)
    # 最低: 國中
    # 若是 xx以上, 最高 None
    if iLen > 1:
        if '高中以下' in iden[0]:
            sqlval['education_min'] = '國中'
        else:
            sqlval['education_min'] = iden[0]
            sqlval['education_max'] = iden[iLen - 1]
    else:
        if '以下' in iden[0]:
            sqlval['education_max'] = iden[0].replace('以下', '')
            sqlval['education_min'] = '國中'
        elif '以上' in iden[0]:
            sqlval['education_min'] = iden[0].replace('以上', '')
        else:
            sqlval['education_min'] = iden[0]
            sqlval['education_max'] = iden[0]


# 接受身分
def etl_status(doc):
    global sqlval
    if not doc:
        return []
    sqlval['accept_status'] = doc

    base = [
        '上班族',
        '應屆畢業生',
        '二度就業',
        '學生實習',
        '日間就讀中',
        '夜間就讀中',
        '外籍人士',
        '原住民',
        '更生人',
        '研發替代役工作'
    ]
    base2 = {'障礙': '身心障礙者',
            '中高齡 : 年滿45歲以上': '中高齡',
            '高齡 : 年滿65歲以上': '高齡'
            }
    item = []
    temp = []
    for iden in doc.split('、'):
        iden = iden.strip()
        for b in base:
            if b in iden and b not in temp:
                temp.append(b)
                item.append(('104', sqlval['job_link'], b))
        for b in base2:
            if b in iden and base2.get(b) not in temp:
                temp.append(base2.get(b))
                item.append(('104', sqlval['job_link'], base2.get(b)))
    return item


# 應徵人數
def etl_intern(doc):
    global sqlval
    if not doc:
        return
    if '0~5' in doc or '6~10' in doc:
        sqlval['candidates'] = 1
    elif '11~30' in doc:
        sqlval['candidates'] = 2
    elif '30人以上' in doc:
        sqlval['candidates'] = 3


if __name__ == "__main__":

    tStart = time.time()  # 計時開始
    start = time.strftime("%m-%d %H:%M:%S")

    # mysql
    mydb = myConnect()
    sql = "SELECT jobL,jobM,jobname,jobname1 FROM category_job "
    cgcomb = mydb.query(sql)
    cg104 = {}
    for cg in cgcomb:
        if cg[2]:
            # cg104[jobname] = {L:jobL,M:jobM}
            cg104[cg[2]] = {
                'L': cg[0],
                'M': cg[1]
            }
    # 清空mysql job
    #mydb.execute("truncate table job")
    mydb.execute("delete from job where source='104'")
    mydb.execute("delete from job_status where source='104'")

    df_errlog = pd.DataFrame(columns=["ERR", "SQL", "VALUES"])
    okcnt = 0
    errcnt = 0
    move = dict.fromkeys((ord(c) for c in u"\xa0\n\t\u3000\u00A0"))

    # mongodb
    client = MongoClient('localhost', 27017)
    db = client.club
    #collects = db.jobs.find({})
    for doc in db.jobs_104.find({}, no_cursor_timeout = True):
        print(doc.get('職缺網址'))
        sqlval = {}
        sqlval['source'] = doc.get('source')
        sqlval['job_link'] = doc.get('職缺網址')
        sqlval['company_link'] = doc.get('公司連結')
        sqlval['title'] = doc.get('職務名稱')
        # 工作內容
        workcontent = doc.get('工作內容')
        if workcontent:
            # 英文描述保留半形空白 不做"".join(x.split())
            if workcontent.get('工作內容'):
                sqlval['content'] = workcontent.get('工作內容').translate(move)
            if workcontent.get('上班地點'):
                sqlval['location'] = workcontent.get('上班地點')
            etl_category(workcontent.get('職務類別'))
            etl_manage(workcontent.get('管理責任'))
            etl_worktime(workcontent.get('上班時段'))
            etl_salary(workcontent.get('工作待遇'))
            etl_quantity(workcontent.get('需求人數'))

            sqlval['expatriate'] = "No"
            if workcontent.get("出差外派"):
                if "無需" not in workcontent.get("出差外派"):
                    sqlval['expatriate'] = "Yes"

            sqlval['holiday_time'] = '公司規定'
            if '週休二日' in workcontent.get("休假制度"):
                sqlval['holiday_time'] = '週休二日'

        # 條件要求
        condition = doc.get('條件要求')
        if condition:
            etl_education(condition.get('學歷要求'))
            jobStatus = etl_status(condition.get('接受身份'))

            if condition.get('科系要求'):
                sqlval['accept_department'] = condition.get('科系要求')

            sqlval['accept_experience'] = 0
            exyear = condition.get('工作經歷').replace('年以上', '')
            if exyear.isdigit():
                sqlval['accept_experience'] = int(exyear)

            if condition.get('擅長工具'):
                sqlval['tool'] = condition.get('擅長工具').translate(move)
            if condition.get('工作技能'):
                sqlval['skill'] = condition.get('工作技能').translate(move)
            if condition.get('具備證照'):
                sqlval['license'] = condition.get('具備證照')
            if condition.get('具備駕照'):
                sqlval['driver_license'] = condition.get('具備駕照')
            if condition.get('其他條件'):
                sqlval['other_requirement'] = condition.get('其他條件').translate(move)

        if doc.get('更新時間'):
            sqlval['modate'] = doc.get('更新時間')

        if doc.get('應徵人數'):
            etl_intern(doc.get('應徵人數'))

        # insert into mysql
        s = '%s,' * len(sqlval.keys())
        sql = 'insert into job (' + ', '.join(sqlval.keys()) + ') values (' + s[:-1] + ')'
        try:
            mydb.execute(sql, tuple(sqlval.values()))
            if jobStatus:
                sql = "insert into job_status (source, job_link, accept_status) values (%s,%s,%s)"
                mydb.executemany(sql, jobStatus)
            mydb.commit()
            okcnt += 1
        except pymysql.err.IntegrityError as err:
            print(err, sql, tuple(sqlval.values()))
            errlog = {
                "ERR": err,
                "SQL": sql,
                "VALUES": tuple(sqlval.values())
            }
            df_errlog = df_errlog.append(errlog, ignore_index=True)
            errcnt += 1
        except pymysql.err.DataError as err:
            print(err, sql, tuple(sqlval.values()))
            errlog = {
                "ERR": err,
                "SQL": sql,
                "VALUES": tuple(sqlval.values())
            }
            df_errlog = df_errlog.append(errlog, ignore_index=True)
            errcnt += 1

    mydb.close()
    client.close()

    # save errlog
    if errcnt > 0:
        df_errlog.to_csv('job104_errlog_' + time.strftime("%H%M%S") + '.csv', index=0)

    tEnd = time.time()  # 計時結束
    end = time.strftime("%m-%d %H:%M:%S")
    print('done!! 成功',okcnt,'筆, 失敗',errcnt,'筆')
    print('花費:', (tEnd - tStart), 'sec', (tEnd - tStart)/60, 'min')
    print(start, ' ~ ', end)
