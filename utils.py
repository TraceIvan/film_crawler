import requests
import re
import csv
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool as ThreadPool
from selenium import webdriver
from MyLog import MyLog
from config import dirs
import time
from MyLog import MyLog
import os
import pandas as pd


class getNearYear(object):
    def __init__(self,start_year_month,end_year_month):
        self.base_url="https://www.imdb.com/movies-coming-soon/"
        self.log=MyLog("getIds.log")
        urls = self.getUrls(start_year_month,end_year_month)#'2000-01', '2020-09'
        ids = []
        for url in urls:
            htmlContent = requests.get(url).text
            soup = BeautifulSoup(htmlContent, 'lxml')
            tagList = soup.find('div', attrs={"class": "list detail"})
            tags = tagList.find_all('div', class_="list_item")
            for tag in tags:
                id_pattern = re.compile("/title/(.+)/")
                temp = tag.find('h4').a.attrs['href'].strip()
                id = id_pattern.search(temp).group(1)
                ids.append(id)
            self.log.info("%s gets! ids:%d" % (url, len(ids)))
        ids = list(set(ids))
        ids.sort()
        ids = [[i] for i in ids]
        store_csv = dirs["ORI_ID_DIR"]+"NearYears" + '2020-01' + '_2020-09' + '.csv'
        with open(store_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter='\t')
            writer.writerows(ids)

    def getUrls(self,st_time, ed_time):
        urls = []
        names = self.getNames(st_time, ed_time)
        for name in names:
            urls.append(self.base_url + name)
        return urls

    def getNames(self,st_time, ed_time):
        names = [st_time]
        st_time = st_time.split('-')
        ed_time = ed_time.split('-')
        st_time = list(map(int, st_time))
        ed_time = list(map(int, ed_time))
        st_year, st_month = st_time[0], st_time[1]
        while st_year <= ed_time[0]:
            st_month = st_month % 12 + 1
            if st_year < ed_time[0] or (st_year == ed_time[0] and st_month <= ed_time[1]):
                names.append('-'.join([str(st_year), str(st_month).zfill(2)]))
            else:
                break
            if st_month == 12:
                st_year += 1
        return names

class getEvents(object):
    def __init__(self):
        base_url = "https://www.imdb.com/event/all"
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('window-size=1920x1080')
        driver = None
        while True:
            try:
                driver = webdriver.Chrome(chrome_options=chrome_options)
                driver.get(base_url)
                driver.implicitly_wait(20)
                # driver = webdriver.Firefox(firefox_options=chrome_options)
                break
            except Exception as e:
                print("selenium: 获取driver失败，重试中(%s)" % e)
        html = driver.page_source
        soup = BeautifulSoup(html, 'lxml')
        list = soup.find('ul', attrs={"class": "event-list__events"})
        lis = list.find_all('li')
        print("events:%d" % len(lis))
        ev_ids = []
        for li in lis:
            cur_id = re.search('(ev\d+)', li.a.attrs['href']).group(1).strip()
            ev_ids.append([cur_id])
        print("get events:%d" % len(ev_ids))
        with open(dirs["ORI_ID_DIR"]+'AllEvents.csv', 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter='\t')
            writer.writerows(ev_ids)
        driver.close()
        driver.quit()

class getTopLowest(object):
    def __init__(self):
        ids1 = self.getTopRatedMovies()
        ids2 = self.getTopRatedEnglishMovies()
        ids3 = self.getMostPopularMovies()
        ids4 = self.getMostPopularTVShows()
        ids5 = self.getTopRatedTVShows()
        ids6 = self.getTopRatedIndianMovies()
        ids7 = self.getLowestRatedMovies()
        ids = []
        ids.extend(ids1)
        ids.extend(ids2)
        ids.extend(ids3)
        ids.extend(ids4)
        ids.extend(ids5)
        ids.extend(ids6)
        ids.extend(ids7)
        ids = list(set(ids))
        ids.sort()
        ids = [[i] for i in ids]
        with open(dirs["ORI_ID_DIR"]+'TopLowest.csv', 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter='\t')
            writer.writerows(ids)

    def getTopRatedMovies(self):
        base_url = "https://www.imdb.com/chart/top"
        ids = []
        answer = []
        htmlContent = requests.get(base_url).text
        soup = BeautifulSoup(htmlContent, 'lxml')
        list = soup.find('tbody', attrs={"class": "lister-list"})
        list = list.find_all('tr')
        for li in list:
            title = li.find('td', attrs={"class": "titleColumn"})
            href = title.a.attrs['href']
            id = re.search('(tt\d+)', href).group(1)
            ids.append([id])
            answer.append(id)
        with open('TopRatedMovies.csv', 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter='\t')
            writer.writerows(ids)
        return answer

    def getTopRatedEnglishMovies(self):
        base_url = "https://www.imdb.com/chart/top-english-movies"
        ids = []
        answer = []
        htmlContent = requests.get(base_url).text
        soup = BeautifulSoup(htmlContent, 'lxml')
        list = soup.find('tbody', attrs={"class": "lister-list"})
        list = list.find_all('tr')
        for li in list:
            title = li.find('td', attrs={"class": "titleColumn"})
            href = title.a.attrs['href']
            id = re.search('(tt\d+)', href).group(1)
            ids.append([id])
            answer.append(id)
        with open('TopRatedEnglishMovies.csv', 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter='\t')
            writer.writerows(ids)
        return answer

    def getMostPopularMovies(self):
        base_url = "https://www.imdb.com/chart/moviemeter"
        ids = []
        answer = []
        htmlContent = requests.get(base_url).text
        soup = BeautifulSoup(htmlContent, 'lxml')
        list = soup.find('tbody', attrs={"class": "lister-list"})
        list = list.find_all('tr')
        for li in list:
            title = li.find('td', attrs={"class": "titleColumn"})
            href = title.a.attrs['href']
            id = re.search('(tt\d+)', href).group(1)
            ids.append([id])
            answer.append(id)
        with open('MostPopularMovies.csv', 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter='\t')
            writer.writerows(ids)
        return answer

    def getMostPopularTVShows(self):
        base_url = "https://www.imdb.com/chart/tvmeter"
        ids = []
        answer = []
        htmlContent = requests.get(base_url).text
        soup = BeautifulSoup(htmlContent, 'lxml')
        list = soup.find('tbody', attrs={"class": "lister-list"})
        list = list.find_all('tr')
        for li in list:
            title = li.find('td', attrs={"class": "titleColumn"})
            href = title.a.attrs['href']
            id = re.search('(tt\d+)', href).group(1)
            ids.append([id])
            answer.append(id)
        with open('MostPopularTVShows.csv', 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter='\t')
            writer.writerows(ids)
        return answer

    def getTopRatedTVShows(self):
        base_url = "https://www.imdb.com/chart/toptv"
        ids = []
        answer = []
        htmlContent = requests.get(base_url).text
        soup = BeautifulSoup(htmlContent, 'lxml')
        list = soup.find('tbody', attrs={"class": "lister-list"})
        list = list.find_all('tr')
        for li in list:
            title = li.find('td', attrs={"class": "titleColumn"})
            href = title.a.attrs['href']
            id = re.search('(tt\d+)', href).group(1)
            ids.append([id])
            answer.append(id)
        with open('TopRatedTVShows.csv', 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter='\t')
            writer.writerows(ids)
        return answer

    def getTopRatedIndianMovies(self):
        base_url = "https://www.imdb.com/india/top-rated-indian-movies/"
        ids = []
        answer = []
        htmlContent = requests.get(base_url).text
        soup = BeautifulSoup(htmlContent, 'lxml')
        list = soup.find('tbody', attrs={"class": "lister-list"})
        list = list.find_all('tr')
        for li in list:
            title = li.find('td', attrs={"class": "titleColumn"})
            href = title.a.attrs['href']
            id = re.search('(tt\d+)', href).group(1)
            ids.append([id])
            answer.append(id)
        with open('TopRatedIndianMovies.csv', 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter='\t')
            writer.writerows(ids)
        return answer

    def getLowestRatedMovies(self):
        base_url = "https://www.imdb.com/chart/bottom"
        ids = []
        answer = []
        htmlContent = requests.get(base_url).text
        soup = BeautifulSoup(htmlContent, 'lxml')
        list = soup.find('tbody', attrs={"class": "lister-list"})
        list = list.find_all('tr')
        for li in list:
            title = li.find('td', attrs={"class": "titleColumn"})
            href = title.a.attrs['href']
            id = re.search('(tt\d+)', href).group(1)
            ids.append([id])
            answer.append(id)
        with open('LowestRatedMovies.csv', 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter='\t')
            writer.writerows(ids)
        return answer

    def getLowestRatedMovies(self):
        base_url = "https://www.imdb.com/chart/bottom"
        ids = []
        answer = []
        htmlContent = requests.get(base_url).text
        soup = BeautifulSoup(htmlContent, 'lxml')
        list = soup.find('tbody', attrs={"class": "lister-list"})
        list = list.find_all('tr')
        for li in list:
            title = li.find('td', attrs={"class": "titleColumn"})
            href = title.a.attrs['href']
            id = re.search('(tt\d+)', href).group(1)
            ids.append([id])
            answer.append(id)
        with open('LowestRatedMovies.csv', 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter='\t')
            writer.writerows(ids)
        return answer
class wrongcsv(object):

    def __init__(self):
        self.THREADS=8
        self.wrongcnt=0
        self.log = MyLog('wrong.log')
        self.dirs={0:{'dir':dirs["IMDB_FILMS_DIR"],"pattern":"tt","list":["imgs.csv","videos.csv","companies.csv","awards.csv","base.csv"],"keys":["ID","ID","ID","ID",'Gross']},
                   #1:{'dir':dirs["IMDB_FILMS_REVIEWS_DIR"],"pattern":"tt","list":["reviews.csv"],"keys":["ID"]},
                   #2:{'dir':dirs["IMDB_FILMS_NEWS_DIR"],"pattern":"tt","list":["news.csv"],"keys":["img_url"]},
                   #3:{'dir':dirs["IMDB_FILMS_IMGS_DIR"],"pattern":"tt","list":["large_img_info.csv"],"keys":["ID"]},
                   #4:{'dir':dirs["IMDB_NM_DIR"],"pattern":"nm","list":["base.csv","awards.csv","imgs.csv","videos.csv"],
                   #   "keys":['id',"ID","ID","ID"]},
                   #5:{'dir':dirs["IMDB_NM_NEWS_DIR"],"pattern":"nm","list":["news.csv"],"keys":["img_url"]}
                   }


        for i in self.dirs.keys():
            self.wrongIDList=[]
            self.cur_dir=self.dirs[i]
            self.files=self.getFiles(self.cur_dir['dir'],self.cur_dir['pattern'])
            epoches=len(self.files)//(self.THREADS*320)
            if len(self.files)%(self.THREADS*320):
                epoches+=1

            for cur_epoch in range(epoches):
                start_epoch=cur_epoch*(self.THREADS*320)
                end_epoch=(cur_epoch+1)*(self.THREADS*320)
                self.log.info("start epoch %d:%d-%d"%(cur_epoch,start_epoch,end_epoch))
                pool = ThreadPool(processes=self.THREADS)
                pool.map(self.check, self.files[start_epoch:end_epoch])
                pool.close()
                pool.join()

            self.log.info('%s: check complete!'%self.cur_dir['dir'])
            self.wrongIDList=list(set(self.wrongIDList))
            for i in range(len(self.wrongIDList)):
                self.wrongIDList[i]=[self.wrongIDList[i]]
            self.log.info("%s: get %d"%(self.cur_dir['dir'],len(self.wrongIDList)))
            with open(dirs['ORI_ID_DIR']+self.cur_dir['dir'][:-1]+'_check.csv', 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter='\t')
                writer.writerows(self.wrongIDList)

    def check(self,id_dir):
        sz=len(self.cur_dir['list'])
        for pos in range(sz):
            csv_dir=id_dir+'/'+self.cur_dir['list'][pos]
            data=pd.read_csv(csv_dir,sep='\t')
            if self.cur_dir['keys'][pos] not in data.keys():
                self.wrongIDList.append(id_dir.split('/')[-1])
                break
    def getFiles(self,pre_dir,pattern):
        files = []
        count = 0
        for fn in os.listdir(pre_dir):
            if re.match(pattern, fn) and os.path.isdir(pre_dir + fn):
                count += 1
                files.append(pre_dir + fn)
        self.log.info("get titles:%d" % count)
        return files

class Summary(object):
    def __init__(self):
        self.dirs={'data/':['imgs.csv','videos.csv','summaries.csv','synopsis.csv','casts.csv','keywords.csv',
                            'release.csv','technicals.csv','companies.csv','locations.csv','awards.csv',
                            'trivia.csv','goofs.csv','quotes.csv','crazycredits.csv','alternateversions.csv',
                            'soundtracks.csv','externalreview.csv','faqs.csv','movieConnections.csv','parentalGuide.csv','base.csv'],
                   'data_news/':['news.csv'],'data_Reviews/':['reviews.csv'],'data_TagsForLargeImg/':['large_img_info.csv']
                   }
        # self.dirs = {
        #     'data_nms/': ['base.csv', 'imgs.csv', 'videos.csv', 'hists.csv', 'bio.csv', 'awards.csv', 'otherworks.csv',
        #                   'publicity.csv', 'external_sites.csv'],
        #     'data_nms_news/': ['news.csv']}
        self.dirs_lists=[]
        self.getAll()
        tot_files=len(self.dirs_lists)
        self.start_time = time.time()
        self.log = MyLog('sum.log')
        self.log.info("tot files:%d"%tot_files)
        self.tot_samples=0
        self.tot_attribute=0
        self.THREADS=8
        epochs = tot_files // (self.THREADS*3200)
        if tot_files % (self.THREADS*3200):
            epochs += 1
        self.log.info("tot epochs:%d"%epochs)
        for i in range(epochs):
            start_id = i * (self.THREADS*3200)
            end_id = (i + 1) * (self.THREADS*3200)
            self.log.info("start:%d--%d,tots:%d" % (start_id, end_id,tot_files))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.sum_file, self.dirs_lists[start_id:end_id])
            pool.close()
            pool.join()
        self.log.error("tot nodes:%d,tot attributes:%d"%(self.tot_samples,self.tot_attribute))
        total_time=time.time()-self.start_time
        self.log.error("tot time:{:.0f}m {:.0f}s".format(total_time // 60, total_time % 60))


    def sum_file(self,file):
        if os.path.exists(file):
            with open(file, 'r', encoding="utf8") as fi:
                lines = fi.readlines()
                if len(lines):
                    try:
                        self.tot_samples += (len(lines) - 1)
                        attributes=len(lines[0].split('\t'))
                        #print(file,attributes)
                        self.tot_attribute+=attributes*(len(lines) - 1)
                    except:
                        self.log.error("%s is wrong"%file)
                else:
                    self.log.error("%s is wrong"%file)
        else:
            self.log.error("%s is not exists"%file)

    def getAll(self):
        tmp=[]
        for key,values in self.dirs.items():
            lists=list(os.listdir(key))
            pool = ThreadPool(processes=self.THREADS)
            self.key=key
            self.values=values
            pool.map(self.getEach, lists)
            pool.close()
            pool.join()
    def getEach(self,fn):
        tmp=[]
        if re.match("tt\d+", fn) and os.path.isdir(self.key + fn):
            for j in self.values:
                tmp.append(self.key+fn+'/'+j)
        self.dirs_lists.extend(tmp)


def delete_dir(cur_dir):
    ls=os.listdir(cur_dir)
    for i in ls:
        c_path=os.path.join(cur_dir,i)
        if os.path.isdir(c_path):
            delete_dir(c_path)
        else:
            os.remove(c_path)
    os.rmdir(cur_dir)

def get_cur_id_list(pre_dir,pattern,file_name):
    files = []
    count = 0
    for fn in os.listdir(pre_dir):
        if re.match(pattern, fn) and os.path.isdir(pre_dir + fn):
            count += 1
            files.append(fn)
    files.sort()
    print("get titles:%d" % count)
    data=pd.DataFrame(files)
    data.to_csv(dirs['ORI_ID_DIR']+file_name,index=False,header=None)
    return files


