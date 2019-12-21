"""

傳入職缺網址，ETL 薪資預測模型所需資料欄位，回傳DataFrame

return :
    來源,職務大類,管理責任,出差外派,工作時間,最高薪資,最低薪資,工作經歷,最低學歷,應徵人數,工作地點區域,產業類型大類(company),產業類型中類(company)
    source,category_top,management,expatriate,work_time,salary_max,salary_min,accept_experience,education_min,candidates,area,industrial_top,industrial_mid
    ex: 104,管理財經,No,No,日班,32000.0,28000.0,3,高中,0~10人應徵,雲嘉南,文教傳播,教育服務
"""

import re
import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup
import lxml
import pandas as pd
import csv
import json
import time


### 爬取104職缺的網頁資訊(針對一個url)|output:dictionary ###
def crawlerJob104(url):
    # 找到內容網址
    head = {'User-Agent': 'Mozilla/5.0'}
    res = requests.get(url, headers=head)
    time.sleep(0.3)  # 休息供網頁讀取
    html = BeautifulSoup(res.text, 'lxml')
    saved = {}

    # 資料區塊
    sec = html.find_all("section", class_="info")  # 主要區塊list
    #if sec == None:
    if not sec:
        saved["Oops"] = "已經結束徵才"
        return {}

    s1 = sec[0]  # 工作內容區塊
    s2 = sec[1]  # 條件要求區塊

    # 開始抓資料囉：公司名稱、應徵人數
    compl = html.find("a", class_="cn")["href"]
    vol = html.find("div", class_="sub").find_all("a")[0].text.replace("\n", "")
    saved["公司連結"] = compl
    saved["應徵人數"] = vol

    # 開始抓資料囉：工作內容
    workcont = {}
    wclc = s1.find_all("dt")  # [欄位]list
    wcl = s1.find_all("dd")  # [資料]list

    wc = s1.find("div", class_="content").find("p")
    wc = wc.prettify().replace("<p>", "").replace("</p>", "").replace("<br/>", "").replace("\n", "").strip()
    workcont["工作內容"] = wc

    for i in range(len(wclc)):
        if "職務類別" in wclc[i].text:
            jc = wcl[i].find_all("span", class_="")
            jc = str(jc).replace("<span>", "").replace("</span>", "").replace("[", "").replace("]", "")
            workcont["職務類別"] = jc
        elif "工作待遇" in wclc[i].text:
            sa = wcl[i].text.split("\n")[0].strip()
            workcont["工作待遇"] = sa
        elif "上班地點" in wclc[i].text:
            lc = wcl[i].prettify().split("\n")[1].strip()
            workcont["上班地點"] = lc
        else:
            culumn = wclc[i].text.replace("：", "")
            workcont[culumn] = wcl[i].text.replace("\n", "").strip()

    saved["工作內容"] = workcont

    # 開始抓資料囉：條件要求
    workreq = {}
    wqc = s2.find_all("dt")  # [欄位]list
    wql = s2.find_all("dd")  # [資料]list

    # 條件列表與內容(每個職缺不同，故寫迴圈)
    for i in range(len(wql)):
        culumn = wqc[i].text.replace("：", "")
        workreq[culumn] = wql[i].text.replace("\n", "").replace("\r", "").strip()

    saved["條件要求"] = workreq

    return saved


### 爬取1111職缺的網頁資訊(針對一個url)|output:dictionary ###
def crawlerJob1111(url):
    jobno = url.split('/')[-2]
    res = requests.get(url)
    html = BeautifulSoup(res.text, 'lxml')

    # data dict
    saved = {}

    # 職稱
    logo = html.find("div", class_="logoTitle")
    if logo == None:
        saved["Oops"] = "已經結束徵才"
        return {}

    #saved['title'] = logo.find("h1").text
    # 公司link
    companylink = logo.find("li", class_="ellipsis").find("a")["href"]
    saved['公司連結'] = "https://www.1111.com.tw" + companylink

    incontent = html.find("section", id="incontent")

    # 工作內容
    cont1 = {}
    workcont = incontent.find("ul", class_="dataList")

    wcontLis = workcont.find_all("li")
    for li in wcontLis:
        try:
            lli = li.find_all("div")
            lliname = lli[0].text.replace('：', '')
            ds = lli[1].find_all("div", class_="cateshow")
            for d in ds:
                d.extract()
            ds = lli[1].find_all("a")
            for d in ds:
                d.extract()
            cont1[lliname] = (lli[1].text.replace(' ', '')).replace('　', '')
        except:
            pass
    saved['工作內容'] = cont1

    # 工作條件
    cont3 = {}
    condi = incontent.find("article", class_="boxsize")
    wcondiLis = condi.find_all("li")
    for li in wcondiLis:
        try:
            lli = li.find_all("div")
            lliname = lli[0].text.replace('：', '')

            cont3[lliname] = lli[1].text.strip()
        except:
            pass
    saved['要求條件'] = cont3

    # 應徵人數
    eurl = "https://www.1111.com.tw/includesU/employeeShowRecruits.asp"
    datas = {'eNo': jobno}
    eres = requests.post(eurl, data=datas)
    eres.encoding = 'utf-8'
    r = BeautifulSoup(eres.text, 'lxml')
    saved['應徵人數'] = r.find_all("span")[-1].text

    return saved


### 爬取104公司的資訊(針對一個url)|output:dictionary ###
def crawlerCompany104(url):
    saved = {}

    # 找到內容網址
    json_url = "https://www.104.com.tw/company/ajax/content/" + url.split("/")[4].split("?")[0]
    res = urlopen(json_url)
    time.sleep(0.3)
    data = json.load(res)

    # 找到資料位置
    saved["產業類型"] = data["data"]["industryDesc"]

    return saved


### 爬取1111公司的資訊(針對一個url)|output:dictionary ###
def crawlerCompany1111(url):
    cont = {}
    try:
        res = requests.get(url)
        html = BeautifulSoup(res.text, 'lxml')
        dataList = html.find("ul", class_="dataList")
        base = dataList.find_all("li")
        for li in base:
            try:
                lli = li.find_all("div")
                lliname = lli[0].text.replace('：', '')
                ds = lli[1].find_all("a", id="showmap")
                for d in ds:
                    d.extract()
                cont[lliname] = (lli[1].text.replace(' ', '')).replace('　', '')
            except:
                pass
    except:
        pass

    return cont


# category_top dict
def getCategoryDict(source):
    if source == '1111':
        csvfile = './data/category_job1111.csv'
    else:
        csvfile = './data/category_job104.csv'
    cg_dict = {}
    with open(csvfile, encoding='utf8') as f:
        rows = csv.reader(f)
        for row in rows:
            cg_dict[row[0]] = row[1]
    return cg_dict


def get_area(location):
    # 建立area字典，取代原area資料，字典沒有的則統一為海外
    area_dict = {'台北': '北北基', '新北': '北北基', '基隆': '北北基',
                 '桃園': '桃竹苗', '新竹': '桃竹苗', '苗栗': '桃竹苗',
                 '台中': '中彰投', '彰化': '中彰投', '南投': '中彰投',
                 '雲林': '雲嘉南', '嘉義': '雲嘉南', '台南': '雲嘉南',
                 '高雄': '高高屏', '屏東': '高高屏',
                 '宜蘭': '宜花東', '花蓮': '宜花東', '台東': '宜花東'}
    area = area_dict.get(location[:2])
    if not area:
        area = '海外'
    return pd.Series([area])


def get_worktime(source, doc):
    result = ''
    doc = doc.replace('上班', '')
    doc = doc.replace('下班', '')
    doc = doc.replace('加班', '')
    doc = doc.replace('常日班', '')
    doc = doc.replace('白天班', '')
    doc = "".join(doc.split())
    cond1 = doc.count('班') > 1 or '輪班' in doc
    cond2 = (doc.count('班') > 1 and '日班日班' not in doc and '不需輪班' not in doc) \
            or True in [x in doc for x in ['輪班', '假日班', '短期']]

    if (source == '104' and cond1) or (source == '1111' and cond2):
        result = '輪班'
    else:
        if '日班' in doc:
            result = '日班'
        elif '大夜班' in doc:
            result = '大夜班'
        elif '晚班' in doc or '中班' in doc or '中班' in doc:
            result = '晚班'
        elif 'AM' in doc:
            result = '日班'
        elif 'PM' in doc:
            result = '晚班'
        elif '~' in doc:
            whour = int(doc.split('~')[0].split(':')[0])
            if whour >= 5 and whour <= 12:
                result = '日班'
            elif whour >= 13 and whour <= 19:
                result = '晚班'
            else:
                result = '大夜班'
        else:
            result = '輪班'

    return pd.Series([result])


def get_categoryTop(source, str):
    result = ''
    cgmap = getCategoryDict(source)
    for doc in str.split('、'):
        for d in doc.split(', '):
            d = d.strip()
            d = d.replace('教育訓綀人員','教育訓練人員')
            d = d.replace('秘書','祕書')
            d = d.replace('/', '／')
            if cgmap.get(d):
                match = True
                result = cgmap.get(d)
                break
        # 取得第一個符合就離開
        if match:
            break
    return pd.Series([result])


def get_manage(manage):
    result = "No"
    if manage:
        if ("不需" in manage) or ("不須" in manage) or ("未定" in manage):
            result = "No"
        else:
            result = "Yes"
    return pd.Series([result])


def get_salary_104(doc):
    result = [0, 0] # [min,max]
    if '月薪' in doc:
        salary = doc.replace(',', '').split('~')
        if len(salary) > 1:
            result[0] = salary[0].split(' ')[1]
            result[1] = salary[1].split('元')[0]
        else:
            min = salary[0].split(' ')[1]
            min = min.replace('元', '').replace('以上', '')
            result[0] = min
            result[1] = min
    elif '年薪' in doc:
        salary = doc.replace(',', '').split('~')
        if len(salary) > 1:
            result[0] = int(salary[0].split(' ')[1]) // 12
            result[1] = int(salary[1].split('元')[0]) // 12
        else:
            min = salary[0].split(' ')[1]
            min = min.replace('元', '').replace('以上', '')
            min = int(min) // 12
            result[0] = min
            result[1] = min
    return result


def get_salary_1111(doc):
    result = [0, 0] # [min,max]
    if '月薪' in doc and doc != '月薪':
        salary = doc.replace(',', '').split('元至')
        if len(salary) > 1:
            result[0] = salary[0].split('月薪')[1].replace(')', '')
            result[1] = salary[1].split('元')[0]
        else:
            min = salary[0].split('月薪')[1].replace(')', '').replace('元', '').replace('以上', '')
            result[0] = min
            result[1] = min
    elif '年薪' in doc and doc != '年薪':
        salary = doc.replace(',', '').split('元至')
        if len(salary) > 1:
            result[0] = int(salary[0].split('年薪')[1].replace(')', '')) // 12
            result[1] = int(salary[1].split('元')[0]) // 12
        else:
            min = salary[0].split('年薪')[1].replace(')', '').replace('元', '').replace('以上', '')
            result[0] = int(min) // 12
            result[1] = int(min) // 12
    return result


def get_experi_1111(doc):
    result = 0
    if doc:
        yy = doc.split('年')
        if yy[0].isdigit():
            result = int(yy[0])
        elif '限定' in yy[0]:
            yy1 = yy[0].split('限定 ')[1]
            if yy1.isdigit():
                result = int(yy1)

    return pd.Series([result])


def get_educationMin_104(doc):
    # 最低: 國中
    min = '國中'
    if not doc:
        return pd.Series([min])

    iden = doc.split('、')
    iLen = len(iden)
    if iLen > 1:
        if '高中以下' in iden[0]:
            min = '國中'
        else:
            min = iden[0]
    else:
        if '以下' in iden[0]:
            min = '國中'
        elif '以上' in iden[0]:
            min = iden[0].replace('以上', '')
        else:
            min = iden[0]

    return pd.Series([min])


def get_educationMin_1111(doc):
    # 最低: 國中
    min = '國中'
    if not doc:
        return pd.Series([min])

    dd = doc.split(' ')[0].replace('高中職', '高中').replace('國小/國中', '國中')
    dd = dd.split('以上')[0]
    if '以上' in doc:
        min = dd
    elif '以下' in doc:
        min = '國中'
    else:
        min = dd
    return pd.Series([min])


def get_intern_104(doc):
    result = '0~10人應徵'
    if not doc:
        return pd.Series([result])
    if '0~5' in doc or '6~10' in doc:
        result = '0~10人應徵'
    elif '11~30' in doc:
        result = '11~30人應徵'
    elif '30人以上' in doc:
        result = '31人以上應徵'
    return pd.Series([result])


def get_intern_1111(doc):
    result = '0~10人應徵'
    if not doc:
        return pd.Series([result])
    if '0 ~ 10' in doc:
        result = '0~10人應徵'
    elif '11 ~ 30' in doc:
        result = '11~30人應徵'
    elif '31 ~ 50' in doc or '超過 50' in doc:
        result = '31人以上應徵'
    return pd.Series([result])


def etl_104(df, job_dict):
    df['source'] = pd.Series("104")
    # 工作內容
    workcontent = job_dict.get('工作內容')
    if workcontent:
        if workcontent.get('職務類別'):
            df['category_top'] = get_categoryTop('104', workcontent.get('職務類別'))
        df['management'] = get_manage(workcontent.get('管理責任'))
        df['expatriate'] = pd.Series("No")
        if workcontent.get("出差外派"):
            if "無需" not in workcontent.get("出差外派"):
                df['expatriate'] = pd.Series("Yes")
        if workcontent.get('上班時段'):
            df['work_time'] = get_worktime('104', workcontent.get('上班時段'))
        if workcontent.get('工作待遇'):
            salary = get_salary_104(workcontent.get('工作待遇'))
            df['salary_min'] = pd.Series(salary[0])
            df['salary_max'] = pd.Series(salary[1])
        if workcontent.get('上班地點'):
            df['area'] = get_area(workcontent.get('上班地點'))
    # 要求條件
    condition = job_dict.get('條件要求')
    if condition:
        df['education_min'] = get_educationMin_104(condition.get('學歷要求'))
        df['accept_experience'] = 0
        if condition.get('工作經歷'):
            exyear = condition.get('工作經歷').replace('年以上', '')
            if exyear.isdigit():
                df['accept_experience'] = int(exyear)
    # 應徵人數
    df['candidates'] = get_intern_104(job_dict.get('應徵人數'))

    return df


def etl_1111(df, job_dict):
    df['source'] = pd.Series("1111")
    # 工作內容
    workcontent = job_dict.get('工作內容')
    if workcontent:
        if workcontent.get('職務類別'):
            df['category_top'] = get_categoryTop('1111', workcontent.get('職務類別'))
        df['management'] = get_manage(workcontent.get('管理人數'))
        df['expatriate'] = pd.Series("No")
        if workcontent.get("是否出差"):
            df['expatriate'] = pd.Series("Yes")
        if workcontent.get('工作時間'):
            df['work_time'] = get_worktime('1111', workcontent.get('工作時間'))
        if workcontent.get('工作待遇'):
            salary = get_salary_1111(workcontent.get('工作待遇'))
            df['salary_min'] = pd.Series(salary[0])
            df['salary_max'] = pd.Series(salary[1])
        if workcontent.get('工作地點'):
            df['area'] = get_area(workcontent.get('工作地點'))
    # 要求條件
    condition = job_dict.get('要求條件')
    if condition:
        df['education_min'] = get_educationMin_1111(condition.get('學歷限制'))
        if condition.get('工作經驗'):
            df['accept_experience'] = get_experi_1111(condition.get('工作經驗'))
    # 應徵人數
    df['candidates'] = get_intern_1111(job_dict.get('應徵人數'))

    return df


### 取得公司產業類別 ###
def get_industry(source, dust):
    # [top, mid]
    result = ["暫不提供", "暫不提供"]
    if not dust:
        return result

    if source == '1111':
        csvfile = './data/category_industry1111.csv'
    else:
        csvfile = './data/category_industry104.csv'

    cgmap = {}
    with open(csvfile, encoding='utf8') as f:
        rows = csv.reader(f)
        for row in rows:
            cgmap[row[0]] = {
                'L': row[1],
                'M': row[2]
            }
    if source == '104':
        dust = dust.replace('╱', '／').replace('/', '／')
    else:
        dust = dust.split("、")[0]
        dust = dust.replace('╱', '／').replace('/', '／')

    if cgmap.get(dust):
        result[0] = cgmap.get(dust).get('L')
        result[1] = cgmap.get(dust).get('M')

    return result



if __name__ == "__main__":

    joblink = "https://www.104.com.tw/job/6qjw8?jobsource=pda"
    #joblink = "https://www.1111.com.tw/job/91121006/"
    joblink = "https://www.104.com.tw/job/6f5xo?jobsource=jolist_a_relevance"

    pdcols = [
        'source',
        'category_top',
        'management',
        'expatriate',
        'work_time',
        'salary_max',
        'salary_min',
        'accept_experience',
        'education_min',
        'candidates',
        'area',
        'industrial_top',
        'industrial_mid'
    ]
    basedf = pd.DataFrame(columns=pdcols)

    # 显示所有列
    pd.set_option('display.max_columns', None)


    if re.match("https://www.104.com.tw", joblink):
        job_dict = crawlerJob104(joblink)
        if job_dict:
            basedf = etl_104(basedf, job_dict)
            if job_dict['公司連結']:
                company_dict = crawlerCompany104(job_dict['公司連結'])
                industry = get_industry('104', company_dict['產業類型'])
                basedf['industrial_top'] = pd.Series(industry[0])
                basedf['industrial_mid'] = pd.Series(industry[1])
            #print(basedf['accept_experience'])
            print(basedf.isna().sum())
            print(basedf)
        else:
            print("徵才已經結束")

    elif re.match("https://www.1111.com.tw", joblink):
        job_dict = crawlerJob1111(joblink)
        if job_dict:
            basedf = etl_1111(basedf, job_dict)
            if job_dict['公司連結']:
                company_dict = crawlerCompany1111(job_dict['公司連結'])
                industry = get_industry('1111', company_dict['行業別'])
                basedf['industrial_top'] = pd.Series(industry[0])
                basedf['industrial_mid'] = pd.Series(industry[1])
            #print(basedf)
            #print(basedf.isna().sum())
        else:
            print("徵才已經結束")
    else:
        print('不受理非104或1111的職缺網址')
