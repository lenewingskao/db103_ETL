from pymongo import MongoClient
import pymysql
import time
import pandas as pd

# connect mysql db
from cr_1111.myConnect import myConnect


# 職務類別
def etl_category(str):
    global sqlval, cg1111, df_errlog
    if not str:
        return
    sqlval['category_raw'] = str
    match = False
    for doc in str.split('、'):
        for d in doc.split(', '):
            d = d.strip()
            d = d.replace('教育訓綀人員','教育訓練人員')
            d = d.replace('秘書','祕書')
            d = d.replace('/', '／')
            if cg1111.get(d):
                match = True
                sqlval['category_top'] = cg1111.get(d).get('L')
                sqlval['category_mid'] = cg1111.get(d).get('M')
                sqlval['category_small'] = d
                break
        if match:
            break

    if not match:
        print('職務類別match Failed', str)
        errlog = {
            "ERR": "職務類別match Failed",
            "SQL": sqlval['job_link'],
            "VALUES": str
        }
        df_errlog = df_errlog.append(errlog, ignore_index=True)


# 管理人數
def etl_manage(manage):
    global sqlval
    if manage:
        if ("未定" in manage):
            sqlval['management'] = "No"
        else:
            sqlval['management'] = "Yes"
            doc = manage.replace('人', '')
            mm = doc.split('~')
            if len(mm) > 1:
                sqlval['manage_max'] = int(mm[0])
                sqlval['manage_min'] = int(mm[1])
            else:
                if '以上' in mm[0]:
                    sqlval['manage_min'] = int(mm[0].split('以上')[0])
                    #sqlval['manage_max'] = 999
                elif '以內' in mm[0]:
                    sqlval['manage_min'] = 1
                    sqlval['manage_max'] = int(mm[0].split('以內')[0])
    else:
        sqlval['management'] = "No"


# 工作時間
def etl_worktime(doc):
    doc = doc.replace('上班', '')
    doc = doc.replace('下班', '')
    doc = doc.replace('加班', '')
    doc = doc.replace('常日班', '')
    doc = doc.replace('白天班', '')
    doc = "".join(doc.split())
    if (doc.count('班') > 1 and '日班日班' not in doc and '不需輪班' not in doc) \
            or True in [x in doc for x in ['輪班', '假日班', '短期']]:
        sqlval['work_time'] = '輪班'
    else:
        if '日班' in doc:
            sqlval['work_time'] = '日班'
        elif '大夜班' in doc:
            sqlval['work_time'] = '大夜班'
        elif '晚班' in doc or '中班' in doc:
            sqlval['work_time'] = '晚班'
        elif 'AM' in doc:
            sqlval['work_time'] = '日班'
        elif 'PM' in doc:
            sqlval['work_time'] = '晚班'
        elif '~' in doc:
            whour = int(doc.split('~')[0].split(':')[0])
            if whour >= 5 and whour <= 12:
                sqlval['work_time'] = '日班'
            elif whour >= 13 and whour <= 19:
                sqlval['work_time'] = '晚班'
            else:
                sqlval['work_time'] = '大夜班'
        else:
            sqlval['work_time'] = '輪班'


# 工作待遇
def etl_salary(doc):
    global sqlval
    if '月薪' in doc and doc != '月薪':
        salary = doc.replace(',', '').split('元至')
        if len(salary) > 1:
            sqlval['salary_min'] = salary[0].split('月薪')[1].replace(')', '')
            sqlval['salary_max'] = salary[1].split('元')[0]
        else:
            min = salary[0].split('月薪')[1].replace(')', '').replace('元', '').replace('以上', '')
            sqlval['salary_min'] = min
            sqlval['salary_max'] = min
    elif '年薪' in doc and doc != '年薪':
        salary = doc.replace(',', '').split('元至')
        if len(salary) > 1:
            sqlval['salary_min'] = int(salary[0].split('年薪')[1].replace(')', '')) // 12
            sqlval['salary_max'] = int(salary[1].split('元')[0]) // 12
        else:
            min = salary[0].split('年薪')[1].replace(')', '').replace('元', '').replace('以上', '')
            sqlval['salary_min'] = int(min) // 12
            sqlval['salary_max'] = int(min) // 12


# 需求人數
def etl_quantity(doc):
    global sqlval
    if not doc:
        return
    quantity = doc.replace('人', '').strip().split('~')
    if len(quantity) > 1:
        sqlval['quantity_min'] = int(quantity[0])
        sqlval['quantity_max'] = int(quantity[1])
    elif quantity[0] != '不限':
        sqlval['quantity_min'] = int(quantity[0])
        sqlval['quantity_max'] = int(quantity[0])


def etl_experi(doc):
    global sqlval
    sqlval['accept_experience'] = 0
    yy = doc.split('年')
    if yy[0].isdigit():
        sqlval['accept_experience'] = int(yy[0])
    elif '限定' in yy[0]:
        yy1 = yy[0].split('限定 ')[1]
        if yy1.isdigit():
            sqlval['accept_experience'] = int(yy1)

# 身分類別
def etl_status(doc):
    global sqlval
    if not doc:
        return []
    sqlval['accept_status'] = doc
    mapping = {
        '一般求職者': '上班族',
        '應屆': '應屆畢業生',
        '日間就學中': '日間就讀中',
        '夜間就學中': '夜間就讀中',
        '原住民': '原住民',
        '身心障礙者': '身心障礙者',
        '二度就業': '二度就業',
        '學生實習': '學生實習',
        '外籍人士': '外籍人士',
        '中高齡': '中高齡',
        '更生人': '更生人',
        '研發替代役': '研發替代役工作'
    }
    item = []
    temp = []
    for iden in doc.split('／'):
        for mp in mapping:
            if mp in iden and mapping.get(mp) not in temp:
                temp.append(mapping.get(mp))
                item.append(('1111',sqlval['job_link'],mapping.get(mp)))
    return item


# 學歷限制
def etl_education(doc):
    global sqlval
    if not doc:
        return
    sqlval['accept_education'] = doc
    # 最低: 國中
    # 若是 xx以上, 最高 None
    dd = doc.split(' ')[0].replace('高中職', '高中').replace('國小/國中', '國中')
    dd = dd.split('以上')[0]
    if '以上' in doc:
        sqlval['education_min'] = dd
    elif '以下' in doc:
        sqlval['education_max'] = dd
        sqlval['education_min'] = '國中'
    else:
        sqlval['education_max'] = dd
        sqlval['education_min'] = dd


# 應徵人數
def etl_intern(doc):
    global sqlval
    if not doc:
        return
    if '0 ~ 10' in doc:
        sqlval['candidates'] = 1
    elif '11 ~ 30' in doc:
        sqlval['candidates'] = 2
    elif '31 ~ 50' in doc or '超過 50' in doc:
        sqlval['candidates'] = 3



if __name__ == "__main__":

    tStart = time.time()  # 計時開始
    start = time.strftime("%m-%d %H:%M:%S")

    # mysql
    mydb = myConnect()
    sql = "SELECT jobL,jobM,jobname,jobname1 FROM category_job "
    cgcomb = mydb.query(sql)
    cg1111 = {}
    for cg in cgcomb:
        if cg[3]:
            # cg1111[jobname1] = {L:jobL,M:jobM}
            cg1111[cg[3]] = {
                'L': cg[0],
                'M': cg[1]
            }
    # 清空mysql job
    mydb.execute("delete from job where source='1111'")
    mydb.execute("delete from job_status where source='1111'")

    df_errlog = pd.DataFrame(columns=["ERR", "SQL", "VALUES"])
    okcnt = 0
    errcnt = 0
    move = dict.fromkeys((ord(c) for c in u"\xa0\n\t\u3000\u00A0"))

    # mongodb
    client = MongoClient('localhost', 27017)
    db = client.club
    #collects = db.jobs_1111.find({"Oops":{"$exists":False}})
    for doc in db.jobs_1111.find({"Oops":{"$exists":False}}, no_cursor_timeout = True):
        print(doc.get('joburl'))
        sqlval = {}
        sqlval['source'] = doc.get('source')
        sqlval['job_link'] = doc.get('joburl')
        sqlval['company_link'] = 'https://www.1111.com.tw/corp/'+ str(doc.get('corpid')) +'/'
        sqlval['title'] = doc.get('title')
        if doc.get('update'):
            sqlval['modate'] = doc.get('update')

        # 工作內容
        workcontent = doc.get('工作內容')
        if workcontent:
            # 英文描述保留半形空白 不做"".join(x.split())
            if workcontent.get('description'):
                sqlval['content'] = workcontent.get('description').translate(move)
            if workcontent.get('工作地點'):
                sqlval['location'] = workcontent.get('工作地點')
            etl_category(workcontent.get('職務類別'))
            etl_manage(workcontent.get('管理人數'))
            etl_worktime(workcontent.get('工作時間'))
            etl_salary(workcontent.get('工作待遇'))
            etl_quantity(workcontent.get('需求人數'))

            sqlval['expatriate'] = "No"
            if workcontent.get("是否出差"):
                sqlval['expatriate'] = "Yes"

            if workcontent.get("休假制度"):
                if True in [x in doc for x in ['輪休', '排休']]:
                    sqlval['holiday_time'] = '公司規定'
                else:
                    sqlval['holiday_time'] = '週休二日'

        # 要求條件
        condition = doc.get('要求條件')
        if condition:
            sqlval['accept_experience'] = 0
            etl_experi(condition.get('工作經驗'))
            jobStatus = etl_status(condition.get('身份類別'))
            etl_education(condition.get('學歷限制'))

            if condition.get('科系限制'):
                sqlval['accept_department'] = condition.get('科系限制').replace(', ','、')

            strTool = []
            if condition.get('電腦專長'):
                strTool.append(condition.get('電腦專長').translate(move))
            if condition.get('中打速度'):
                strTool.append(condition.get('中打速度').translate(move))
            if condition.get('英打速度'):
                strTool.append(condition.get('英打速度').translate(move))
            if strTool:
                sqlval['tool'] = "、".join(strTool)

            if condition.get('專業憑證'):
                sqlval['license'] = condition.get('專業憑證').replace(', ','、')

            if condition.get('需有駕照'):
                sqlval['driver_license'] = condition.get('需有駕照')

            if condition.get('附加條件'):
                sqlval['other_requirement'] = condition.get('附加條件').translate(move)

        # 應徵人數
        etl_intern(doc.get('intern'))

        # insert into mysql
        s = '%s,' * len(sqlval.keys())
        sql = 'insert into job (' + ', '.join(sqlval.keys()) + ') values (' + s[:-1] + ')'
        try:
            mydb.execute(sql, tuple(sqlval.values()))
            if jobStatus:
                sql = "insert into job_status (source, job_link, accept_status) values (%s,%s,%s)"
                #print(jobStatus)
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
        df_errlog.to_csv('job1111_errlog_' + time.strftime("%H%M%S") + '.csv', index=0)

    tEnd = time.time()  # 計時結束
    end = time.strftime("%m-%d %H:%M:%S")
    print('done!! 成功', okcnt, '筆, 失敗', errcnt, '筆')
    print('花費:', (tEnd - tStart), 'sec', (tEnd - tStart) / 60, 'min')
    print(start, ' ~ ', end)