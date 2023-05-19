from bs4 import BeautifulSoup
import requests
import time
from multiprocessing.dummy import Pool as ThreadPool
from selenium.webdriver.common.by import By
from seleniumwire import webdriver
import os
import re
import pandas as pd
import csv
import json
from config import dirs
from MyLog import MyLog
from utils import delete_dir
from urllib.request import urlretrieve
import datetime
import shutil


class Item(object):
    movieName = None
    id = None
    IMDB_Rating = None
    Metascore = None
    Gross = None
    imgs = None
    large_img_tag = None
    videos = None
    # directors=None
    # writers=None
    casts = None
    keywords = None
    Summaries = None
    Synopsis = None
    reviews = None
    releaseinfo = None
    # akas=None
    technicals = None
    companies = None
    locations = None
    awards = None
    trivias = None
    goofs = None
    quotes = None
    crazycredits = None
    alternateversions = None
    movieconnections = None
    soundtracks = None
    faqs = None
    parentalguides = None
    externalreviews = None
    news = None
    ratings = None
    # nms
    nmName = None
    jobs = None
    born = None
    hists = None
    bios = None
    otherwork = None
    publicity = None
    external_sites = None
    # event
    year = None
    name = None
    sub_name = None
    event = None
    # companies
    coid = None
    coName = None
    coFilms = None


class baseSpider(object):
    def __init__(self):
        self.timeout = 5
        self.readtimeout = 30
        self.request_sleep = 2
        self.sep = '\t'
        self.Header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:71.0) Gecko/20100101 Firefox/71.0"}
        self.proxy = {"http": "http://127.0.0.1:1080", "https": "https://127.0.0.1:1080"}
        # self.proxy = {"http": "http://127.0.0.1:7890", "https": "https://127.0.0.1:7890"}
        self.log = MyLog('baseSpider.log')

    def get_all_titles(self, file=None, base_name=None):
        if file is None:
            file = dirs["ORI_ID_DIR"] + 'titles.csv'
        else:
            file = dirs["ORI_ID_DIR"] + file
        data = pd.read_csv(file, sep='\t', header=None)
        titles = data.iloc[:, 0].values.tolist()
        titles = list(set(titles))
        titles.sort()
        self.sz = len(titles)
        if base_name is None:
            base_name = 'title'
        for i in range(self.sz):
            titles[i] = "https://www.imdb.com/" + base_name + '/' + titles[i]
        return titles

    def get_cur_titles(self, pre_dir, match_pattern=None, base_name=None):
        titles = []
        if match_pattern is None:
            match_pattern = "tt\d+"
        for fn in os.listdir(pre_dir):
            if re.match(match_pattern, fn) and os.path.isdir(pre_dir + fn):
                titles.append(fn)
        titles.sort()
        self.sz = len(titles)
        if base_name is None:
            base_name = 'title'
        for i in range(self.sz):
            titles[i] = "https://www.imdb.com/" + base_name + '/' + titles[i]
        return titles

    def isok_getUrlInfo(self, url):
        info = self.getResponseContent(url)
        cnt_500 = 0
        if info is None:
            return None
        elif info == "404":
            return None
        elif info == '500':
            cnt_500 += 1
            while cnt_500 < 20:
                info = self.getResponseContent(url)
                if info == '500':
                    cnt_500 += 1
                else:
                    return info
            return None
        else:
            return info

    def getResponseContent(self, url):
        time.sleep(self.request_sleep)
        while True:
            try:
                response = requests.get(url, timeout=(self.timeout, self.readtimeout),
                                        headers=self.Header)  # proxies=self.proxy
                if int(response.status_code) != 200:
                    self.log.error("%s :%d!" % (url, int(response.status_code)))
                    if int(response.status_code) == 404:
                        return "404"
                    if int(response.status_code) == 500:
                        return "500"
                    continue
            except Exception as e:
                self.log.error(e)
            else:
                self.log.info("返回url:%s 成功, status_code:%d" % (url, int(response.status_code)))
                return response.text
        self.log.error("返回url:%s 失败" % url)
        return None


class films_spider(baseSpider):
    def __init__(self, use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS):
        super().__init__()
        self.is_repair = is_repair
        self.use_csv = use_csv
        self.is_reverse = is_reverse
        self.THREADS = THREADS
        self.save_pre_dir = save_dir
        if not os.path.exists(self.save_pre_dir):
            os.makedirs(self.save_pre_dir)
        self.log = MyLog(log_file)
        if self.is_repair:
            self.urls = self.get_cur_titles(self.save_pre_dir, self.use_csv['pattern'], self.use_csv['base_name'])
        else:
            self.urls = self.get_all_titles(self.use_csv['file'], self.use_csv['base_name'])
        if self.is_reverse:
            self.urls.reverse()
        self.start_epoch_file = dirs['SPIDER_CUR_EPOCH_DIR'] + epoch_file
        if not os.path.exists(dirs['SPIDER_CUR_EPOCH_DIR']):
            os.makedirs(dirs['SPIDER_CUR_EPOCH_DIR'])
        self.PER_PAGE_OF_IMG = 48
        self.PER_PAGE_OF_VIDEO = 30
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('window-size=1920x1080')
        self.chrome_options.add_argument('–no-sandbox')
        self.chrome_options.add_argument('disable-cache')  # 禁用缓存
        self.chrome_options.add_argument('–disable-extensions')
        self.chrome_options.add_argument('--incognito')  # 无痕隐身模式
        self.chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])  # 设置为开发者模式
        self.chrome_options.add_argument('log-level=3')  # 禁用大量日志信息滚动输出
        # chrome_options.add_argument('user-agent=%s'%self.Header["User-Agent"])
        # self.chrome_options.add_argument('proxy-server=%s'%self.proxy["http"])
        self.chrome_options.add_argument('--ignore-certificate-errors')
        self.chrome_options.add_argument('--ignore-certificate-errors-spki-list')
        # self.seleniumwire_options = {
        #    'proxy': {
        #        'http': 'http://127.0.0.1:7890',
        #        'https': 'https://127.0.0.1:7890'
        #    }
        # }
        # self.seleniumwire_options = {
        #    'proxy': {
        #        'http': 'http://127.0.0.1:1080',
        #        'https': 'https://127.0.0.1:1080'
        #    }
        # }
        # chrome_options.add_argument('--hide-scrollbars')
        # chrome_options.add_argument('--disable-dev-shm-usage')

    def start_spider(self):
        tot_urls = len(self.urls)
        self.log.info("total ids:%d" % tot_urls)
        epochs = tot_urls // (self.THREADS * 2)
        if tot_urls % (self.THREADS * 2):
            epochs += 1
        start_epoch = 0
        if os.path.exists(self.start_epoch_file):
            with open(self.start_epoch_file, "r", encoding="utf8") as fi:
                lines = fi.readlines()
                start_epoch = int(lines[0]) + 1

        self.start_time = time.time()
        self.end_time = self.start_time
        for i in range(start_epoch, epochs):
            self.sum_dict = {
                "imgs_samples": 0,
                "imgs_attributes": 0,
                "videos_samples": 0,
                "videos_attributes": 0,
                "summaries_samples": 0,
                "summaries_attributes": 0,
                "synopsis_samples": 0,
                "synopsis_attributes": 0,
                "casts_samples": 0,
                "casts_attributes": 0,
                "keywords_samples": 0,
                "keywords_attributes": 0,
                "release_samples": 0,
                "release_attributes": 0,
                "technicals_samples": 0,
                "technicals_attributes": 0,
                "companies_samples": 0,
                "companies_attributes": 0,
                "locations_samples": 0,
                "locations_attributes": 0,
                "awards_samples": 0,
                "awards_attributes": 0,
                "trivia_samples": 0,
                "trivia_attributes": 0,
                "goofs_samples": 0,
                "goofs_attributes": 0,
                "quotes_samples": 0,
                "quotes_attributes": 0,
                "crazycredits_samples": 0,
                "crazycredits_attributes": 0,
                "alternateversions_samples": 0,
                "alternateversions_attributes": 0,
                "soundtracks_samples": 0,
                "soundtracks_attributes": 0,
                "externalreview_samples": 0,
                "externalreview_attributes": 0,
                "faqs_samples": 0,
                "faqs_attributes": 0,
                "movieConnections_samples": 0,
                "movieConnections_attributes": 0,
                "parentalGuide_samples": 0,
                "parentalGuide_attributes": 0,
                "base_samples": 0,
                "base_attributes": 0,
                "data_crawled": 0
            }
            start_id = i * (self.THREADS * 2)
            end_id = (i + 1) * (self.THREADS * 2)
            self.items = []
            self.log.error("start %d:%d--%d" % (i, start_id, end_id))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.spider, self.urls[start_id:end_id])
            pool.close()
            pool.join()
            with open(self.start_epoch_file, "w", encoding="utf8") as fi:
                fi.write(str(i))
            if i % 2 == 0:
                shutil.copy(self.start_epoch_file, self.start_epoch_file + '.bak.txt')

            json_file = "./data_sum_tt_info.json"
            json_bak_file = "./data_sum_tt_info_bak.json"
            if i % 2 == 0:
                shutil.copy(json_file, json_bak_file)
            with open(json_file, 'r') as load_f:
                load_dict = json.load(load_f)
            for cur_key in load_dict.keys():
                load_dict[cur_key] += self.sum_dict[cur_key]
                self.sum_dict[cur_key] = 0
            json_str = json.dumps(load_dict, indent=4)
            self.log.info("sum_info: " + json_str)
            with open(json_file, "w") as f:
                f.write(json_str)

        self.end_time = time.time()
        total_time = self.end_time - self.start_time
        self.log.info('爬取数据信息及链接完成，总用时： {:.0f}m {:.0f}s'.format(total_time // 60, total_time % 60))

    def spider(self, url):
        item = Item()
        item.id = re.search("/(tt\d+)", url).group(1).strip()
        if not self.is_repair:
            base_csv = self.save_pre_dir + item.id + '/base.csv'
            if os.path.exists(base_csv):
                self.log.info("%s 已经爬取" % url)
                return

        externalreview_url = url + "/externalreviews"  # 外部网站评论
        fullcredits_url = url + "/fullcredits"  # director、writer、cast及其图片
        img_url = url + "/mediaindex"  # 电影相关图片
        video_url = url + "/videogallery"  # 电影相关视频
        summary_synopsis_url = url + "/plotsummary"  # 总结、梗概
        keywords_url = url + "/keywords"  # 关键词
        parentalguide_url = url + "/parentalguide"  # 家长指导
        release_url = url + "/releaseinfo"  # 发布日期、地区别名
        companycredits_url = url + "/companycredits"  # 有关公司
        locations_url = url + "/locations"  # 拍摄地点和拍摄日期
        technical_url = url + "/technical"  # 技术指标
        trivia_url = url + "/trivia"  # 琐事
        goofs_url = url + "/goofs"  # 电影中出现的一些错误
        crazycredits_url = url + "/crazycredits"  # crazycredits
        quotes_url = url + "/quotes"  # 引用
        alternateversions_url = url + "/alternateversions"  # 备用版本
        movieconnections_url = url + "/movieconnections"  # 其他电影关系
        soundtrack_url = url + "/soundtrack"  # 原声带
        faq_url = url + "/faq"  # 疑问和解答
        awards_url = url + "/awards"  # 获奖

        htmlContent = self.isok_getUrlInfo(url)
        if htmlContent is None:
            self.log.error("Can't get this url: %s" % url)
            if os.path.exists(self.save_pre_dir + item.id):
                delete_dir(self.save_pre_dir + item.id)
            return

        soup = BeautifulSoup(htmlContent, 'lxml')
        try:
            item.movieName = soup.find('div', attrs={"class": "title_wrapper"}).h1.get_text().strip()
            rating = soup.find('span', attrs={"class": "rating"})
            if rating != None:
                item.IMDB_Rating = rating.get_text().strip()
            else:
                item.IMDB_Rating = ""
            metascore = soup.find('div', attrs={"class": "plot_summary_wrapper"}).find('div', class_="metacriticScore")
            if metascore != None:
                item.Metascore = metascore.get_text().strip()
            else:
                item.Metascore = ""
        except:
            item.movieName = soup.find('h1', attrs={"data-testid": "hero-title-block__title"}).get_text().strip()
            rating = soup.find('div', attrs={"data-testid": "hero-title-block__aggregate-rating__score"})
            if rating != None:
                item.IMDB_Rating = rating.get_text().strip()
            else:
                item.IMDB_Rating = ""
            metascore = soup.find('span', attrs={"class": "score-meta"})
            if metascore != None:
                item.Metascore = metascore.get_text().strip()
            else:
                item.Metascore = ""
        soup.decompose()
        if self.is_repair:
            # 14、引用
            self.getQuotes(item, quotes_url)
            # 17、movieconnections
            self.getMovieconnections(item, movieconnections_url)

        if not self.is_repair:
            # 2、电影相关图片
            html2 = self.isok_getUrlInfo(img_url)
            if html2 is None:
                self.log.error("Can't get this url: %s" % img_url)
                return
            soup2 = BeautifulSoup(html2, 'lxml')
            leftright = soup2.find('div', attrs={"id": "media_index_content", "class": "header"})
            if leftright != None:
                photos = leftright.find('div', attrs={"id": "left", "class": "desc"})
                if photos != None:
                    photos = photos.get_text().strip()
                    nums = re.findall('(\d+)', photos)
                    if len(nums) >= 3:
                        nums = ''.join(nums[2:])
                    else:
                        nums = ''.join(nums)
                    nums = int(nums)
                    self.getAllimgs(item, img_url, nums)
            soup2.decompose()
            # 3、电影相关视频
            html3 = self.isok_getUrlInfo(video_url)
            if html3 is None:
                self.log.error("Can't get this url: %s" % video_url)
                return
            soup3 = BeautifulSoup(html3, 'lxml')
            desc = soup3.find('span', attrs={"id": "vg-left"})
            if desc != None:
                video_nums = desc.get_text().strip()
                video_nums = re.findall('(\d+)', video_nums)
                if len(video_nums) >= 3:
                    video_nums = ''.join(video_nums[2:])
                else:
                    video_nums = ''.join(video_nums)
                video_nums = int(video_nums)
                self.getAllvideos(item, video_url, video_nums)
            soup3.decompose()
            # 11、获奖
            self.getAwards(item, awards_url)
            # 9、有关公司
            self.getCompanies(item, companycredits_url)
            # 4、总结、梗概
            self.getSum_Syn(item, summary_synopsis_url)
            # 5、director、writer、cast及其图片
            self.getCredits(item, fullcredits_url)
            # 6、关键词
            self.getKeywords(item, keywords_url)
            # 7、releaseinfo,AKA(also known as)
            self.getReleaseinfo(item, release_url)
            # 8、技术指标
            self.getTechnical(item, technical_url)
            # 10、拍摄地点和拍摄日期
            self.getLocations(item, locations_url)
            # 12、trivia
            self.getTrivia(item, trivia_url)
            # 13、goofs
            self.getGoofs(item, goofs_url)
            # 14、引用
            self.getQuotes(item, quotes_url)
            # 15、crazycredits
            self.getCrazycredits(item, crazycredits_url)
            # 16、alternateversions
            self.getAlternateversions(item, alternateversions_url)
            # 17、movieconnections
            self.getMovieconnections(item, movieconnections_url)
            # 18、原声带
            self.getSoundtracks(item, soundtrack_url)
            # 19、faqs
            self.getFaqs(item, faq_url)
            # 20、parentalguide
            self.getParentalguide(item, parentalguide_url)
            # 21、externalreview
            self.getExternalreview(item, externalreview_url)

        self.log.info("获取%s结束" % item.movieName)
        self.pipeline_save_url(item)

    def pipeline_save_url(self, item):
        item_dir = self.save_pre_dir + item.id
        if not os.path.exists(item_dir):
            os.makedirs(item_dir)

        if self.is_repair:
            self.save_movieconnections_info(item, item_dir)
            self.save_quotes_info(item, item_dir)
        if not self.is_repair:
            self.save_img_info(item, item_dir)
            self.save_video_info(item, item_dir)
            self.save_companies_info(item, item_dir)
            self.save_awards_info(item, item_dir)
            self.save_summaries_info(item, item_dir)
            self.save_casts_info(item, item_dir)
            self.save_keywords_info(item, item_dir)
            self.save_release_info(item, item_dir)
            self.save_technicals_info(item, item_dir)
            self.save_locations_info(item, item_dir)
            self.save_trivias_info(item, item_dir)
            self.save_goofs_info(item, item_dir)
            self.save_quotes_info(item, item_dir)
            self.save_crazycredits_info(item, item_dir)
            self.save_alternateversions_info(item, item_dir)
            self.save_soundtracks_info(item, item_dir)
            self.save_externalreview_info(item, item_dir)
            self.save_faqs_info(item, item_dir)
            self.save_movieconnections_info(item, item_dir)
            self.save_parentalguide_info(item, item_dir)
        info_csv = item_dir + '/base.csv'  # name,id,IMDB_Rating,Metascore,Gross
        with open(info_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(
                ['id', 'name', 'IMDB_Rating', 'Metascore', 'Gross'])
            writer.writerow([item.id, item.movieName, item.IMDB_Rating, item.Metascore, item.Gross])
            self.sum_dict["base_samples"] += 1
            self.sum_dict["base_attributes"] += 5
            self.sum_dict["data_crawled"] += 1

        self.log.info("%s写入文件成功" % (item.id))

    def save_img_info(self, item, item_dir):
        imgs_csv = item_dir + '/imgs.csv'
        with open(imgs_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["small", "large", "ID", "ori"])
            if item.imgs != None:
                self.log.info("save: %d small pics %d,large pics %d,ori pics %d" % (
                    len(item.imgs["ID"]), len(item.imgs["small"]), len(item.imgs["large"]), len(item.imgs["ori"])))
                all_img = list(zip(item.imgs["small"], item.imgs["large"], item.imgs["ID"], item.imgs["ori"]))
                writer.writerows(all_img)
                self.sum_dict["imgs_samples"] += len(all_img)
                self.sum_dict["imgs_attributes"] += len(all_img) * 4

    def save_video_info(self, item, item_dir):
        videos_csv = item_dir + '/videos.csv'
        with open(videos_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["url", "ID"])
            if item.videos != None:
                self.log.info("save : %d videos %d" % (len(item.videos['ID']), len(item.videos['url'])))
                all_video = list(zip(item.videos["url"], item.videos["ID"]))
                writer.writerows(all_video)
                self.sum_dict["videos_samples"] += len(all_video)
                self.sum_dict["videos_attributes"] += len(all_video) * 2

    def save_companies_info(self, item, item_dir):
        companies_csv = item_dir + '/companies.csv'
        with open(companies_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["type", "name", "ID"])
            if item.companies != None:
                writer.writerows(item.companies)
                self.sum_dict["companies_samples"] += len(item.companies)
                self.sum_dict["companies_attributes"] += len(item.companies) * 3

    def save_awards_info(self, item, item_dir):
        awards_csv = item_dir + '/awards.csv'
        with open(awards_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["title", "ID", "award_descriptions"])
            if item.awards != None:
                writer.writerows(item.awards)
                self.sum_dict["awards_samples"] += len(item.awards)
                self.sum_dict["awards_attributes"] += len(item.awards) * 3

    def save_summaries_info(self, item, item_dir):
        summaries_csv = item_dir + '/summaries.csv'
        with open(summaries_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["text", "author"])
            if item.Summaries != None:
                writer.writerows(item.Summaries)
                self.sum_dict["summaries_samples"] += len(item.Summaries)
                self.sum_dict["summaries_attributes"] += len(item.Summaries) * 2
        Synopsis_csv = item_dir + '/synopsis.csv'
        with open(Synopsis_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["text"])
            if item.Synopsis != None:
                writer.writerows(item.Synopsis)
                self.sum_dict["synopsis_samples"] += len(item.Synopsis)
                self.sum_dict["synopsis_attributes"] += len(item.Synopsis) * 1

    def save_casts_info(self, item, item_dir):
        casts_csv = item_dir + '/casts.csv'
        with open(casts_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["name", "id", "img", "img_label", "character"])
            if item.casts != None:
                writer.writerows(item.casts)
                self.sum_dict["casts_samples"] += len(item.casts)
                self.sum_dict["casts_attributes"] += len(item.casts) * 5

    def save_keywords_info(self, item, item_dir):
        keywords_csv = item_dir + '/keywords.csv'
        with open(keywords_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["keywords"])
            if item.keywords != None:
                writer.writerows(item.keywords)
                self.sum_dict["keywords_samples"] += len(item.keywords)
                self.sum_dict["keywords_attributes"] += len(item.keywords) * 1

    def save_release_info(self, item, item_dir):
        release_csv = item_dir + '/release.csv'
        with open(release_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["country", "date", "attribute"])
            if item.releaseinfo != None:
                writer.writerows(item.releaseinfo)
                self.sum_dict["release_samples"] += len(item.releaseinfo)
                self.sum_dict["release_attributes"] += len(item.releaseinfo) * 3

    def save_technicals_info(self, item, item_dir):
        technicals_csv = item_dir + '/technicals.csv'
        with open(technicals_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["label", "value"])
            if item.technicals != None:
                writer.writerows(item.technicals)
                self.sum_dict["technicals_samples"] += len(item.technicals)
                self.sum_dict["technicals_attributes"] += len(item.technicals) * 2

    def save_locations_info(self, item, item_dir):
        locations_csv = item_dir + '/locations.csv'
        with open(locations_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["type", "value"])
            if item.locations != None:
                writer.writerows(item.locations)
                self.sum_dict["locations_samples"] += len(item.locations)
                self.sum_dict["locations_attributes"] += len(item.locations) * 2

    def save_trivias_info(self, item, item_dir):
        trivia_csv = item_dir + '/trivia.csv'
        with open(trivia_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["text"])
            if item.trivias != None:
                writer.writerows(item.trivias)
                self.sum_dict["trivia_samples"] += len(item.trivias)
                self.sum_dict["trivia_attributes"] += len(item.trivias) * 1

    def save_goofs_info(self, item, item_dir):
        goofs_csv = item_dir + '/goofs.csv'
        with open(goofs_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["text"])
            if item.goofs != None:
                writer.writerows(item.goofs)
                self.sum_dict["goofs_samples"] += len(item.goofs)
                self.sum_dict["goofs_attributes"] += len(item.goofs) * 1

    def save_quotes_info(self, item, item_dir):
        quotes_csv = item_dir + '/quotes.csv'
        with open(quotes_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["id", "character", "text"])
            if item.quotes != None:
                self.log.info("get quotes:%d" % len(item.quotes))
                writer.writerows(item.quotes)
                self.sum_dict["quotes_samples"] += len(item.quotes)
                self.sum_dict["quotes_attributes"] += len(item.quotes) * 3

    def save_crazycredits_info(self, item, item_dir):
        crazycredits_csv = item_dir + '/crazycredits.csv'
        with open(crazycredits_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["text"])
            if item.crazycredits != None:
                writer.writerows(item.crazycredits)
                self.sum_dict["crazycredits_samples"] += len(item.crazycredits)
                self.sum_dict["crazycredits_attributes"] += len(item.crazycredits) * 1

    def save_alternateversions_info(self, item, item_dir):
        alternateversions_csv = item_dir + '/alternateversions.csv'
        with open(alternateversions_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["text"])
            if item.alternateversions != None:
                writer.writerows(item.alternateversions)
                self.sum_dict["alternateversions_samples"] += len(item.alternateversions)
                self.sum_dict["alternateversions_attributes"] += len(item.alternateversions) * 1

    def save_soundtracks_info(self, item, item_dir):
        soundtracks_csv = item_dir + '/soundtracks.csv'
        with open(soundtracks_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["text"])
            if item.soundtracks != None:
                writer.writerows(item.soundtracks)
                self.sum_dict["soundtracks_samples"] += len(item.soundtracks)
                self.sum_dict["soundtracks_attributes"] += len(item.soundtracks) * 1

    def save_externalreview_info(self, item, item_dir):
        externalreview_csv = item_dir + '/externalreview.csv'
        with open(externalreview_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["name", "url"])
            if item.externalreviews != None:
                writer.writerows(item.externalreviews)
                self.sum_dict["externalreview_samples"] += len(item.externalreviews)
                self.sum_dict["externalreview_attributes"] += len(item.externalreviews) * 2

    def save_faqs_info(self, item, item_dir):
        faqs_csv = item_dir + '/faqs.csv'
        with open(faqs_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["question", "answer"])
            if item.faqs != None:
                faqs = [list(i.values()) for i in item.faqs]
                writer.writerows(faqs)
                self.sum_dict["faqs_samples"] += len(faqs)
                self.sum_dict["faqs_attributes"] += len(faqs) * 2

    def save_movieconnections_info(self, item, item_dir):
        movieconnections_csv = item_dir + '/movieConnections.csv'
        with open(movieconnections_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["type", "link_id", "link_name", "text"])
            if item.movieconnections != None:
                connections = [list(i.values()) for i in item.movieconnections]
                self.log.info("get connections:%d" % (len(connections)))
                writer.writerows(connections)
                self.sum_dict["movieConnections_samples"] += len(connections)
                self.sum_dict["movieConnections_attributes"] += len(connections) * 4

    def save_parentalguide_info(self, item, item_dir):
        parentalguide_csv = item_dir + '/parentalGuide.csv'
        with open(parentalguide_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["type", "type2", "text"])
            if item.parentalguides != []:
                writer.writerows(item.parentalguides)
                self.sum_dict["parentalGuide_samples"] += len(item.parentalguides)
                self.sum_dict["parentalGuide_attributes"] += len(item.parentalguides) * 3

    def getAllimgs(self, item, url, nums):
        self.log.info("%s有%s张图片" % (item.id, nums))
        pages = nums // self.PER_PAGE_OF_IMG
        if nums % self.PER_PAGE_OF_IMG:
            pages += 1
        page_url = []
        # orlList=[]
        for i in range(1, pages + 1):
            page_url.append(url + '?page=' + str(i))
        pool = ThreadPool(processes=self.THREADS)
        res = pool.map_async(self.get_curUrl_img, page_url)
        pool.close()
        pool.join()
        res.wait()
        if res.ready():
            if res.successful():
                tmp = res.get()
                # print(tmp)
                item.imgs = {"small": [], "large": [], "ID": [], "ori": []}
                for i in range(pages):
                    item.imgs["small"].extend(tmp[i][0])
                    item.imgs["large"].extend(tmp[i][1])
                    item.imgs["ID"].extend(tmp[i][2])
                    item.imgs["ori"].extend(tmp[i][3])

    def get_curUrl_img(self, cur_url):
        htmlContent = self.isok_getUrlInfo(cur_url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        imgs = soup.find('div', attrs={"class": "media_index_thumb_list", "id": "media_index_thumbnail_grid"})
        imgs = imgs.find_all('img')
        small_list = []
        large_list = []
        ori_list = []
        id_list = []
        for img in imgs:
            img_url = img.attrs['src']
            small_list.append(img_url)
        orlList = []
        for img in imgs:
            img_ori_url = "https://www.imdb.com" + img.parent.attrs['href']
            orlList.append(img_ori_url)
            id = re.search("/(rm\d+)", img_ori_url).group(1).strip()
            id_list.append(id)
        pool = ThreadPool(processes=self.THREADS)
        res = pool.map_async(self.get_ori_imgs, orlList)
        pool.close()
        pool.join()
        res.wait()
        if res.ready():
            if res.successful():
                large_list = res.get()
        while len(small_list) != len(large_list):
            self.log.error("获取大图失败，重新尝试中")
            pool = ThreadPool(processes=self.THREADS)
            res = pool.map_async(self.get_ori_imgs, orlList)
            pool.close()
            pool.join()
            res.wait()
            if res.ready():
                if res.successful():
                    large_list = res.get()
        soup.decompose()
        for i in large_list:
            tmp = i.split('.')
            ori_img = tmp[:3] + tmp[-1:]
            ori_img = '.'.join(ori_img)
            ori_list.append(ori_img)
        return small_list, large_list, id_list, ori_list, orlList

    def get_ori_imgs(self, img_url):
        cnt = 0
        while cnt < 10:
            htmlContent2 = self.isok_getUrlInfo(img_url)
            if htmlContent2 is not None:
                break
            else:
                cnt += 1
        if htmlContent2 is None:
            self.log.error("%s 获取地址出错" % img_url)
            return None
        soup2 = BeautifulSoup(htmlContent2, 'lxml')
        img_ori_url = soup2.find('meta', attrs={"property": "og:image"}).attrs['content']
        if img_ori_url is None:
            self.log.error("%s 获取地址出错" % img_url)
        soup2.decompose()
        return img_ori_url

    def getAllvideos(self, item, url, nums):
        self.log.info("%s有%s个视频" % (item.id, nums))
        pages = nums // self.PER_PAGE_OF_VIDEO
        if nums % self.PER_PAGE_OF_VIDEO:
            pages += 1
        page_url = []
        for i in range(1, pages + 1):
            page_url.append(url + '?page=' + str(i))
        item.videos = {'url': [], "ID": []}
        tmp_videos = []
        for cur_url in page_url:
            htmlContent = self.isok_getUrlInfo(cur_url)
            soup = BeautifulSoup(htmlContent, 'lxml')
            videos = soup.find('div', attrs={"class": re.compile("search-results")})
            videos = videos.find_all('div', attrs={"class": re.compile("results-item")})
            tmp_videos.extend(videos)
            soup.decompose()
        pool = ThreadPool(processes=self.THREADS)
        res = pool.map_async(self.get_cur_video, tmp_videos)
        pool.close()
        pool.join()
        res.wait()
        if res.ready():
            if res.successful():
                tmp = res.get()
                for i in range(len(tmp)):
                    if tmp[i] is None:
                        continue
                    item.videos['url'].append(tmp[i][0])
                    item.videos['ID'].append(tmp[i][1])
        self.log.info("%s tot videos:%d" % (item.id, len(item.videos['url'])))

    def get_cur_video(self, video):
        video_url = video.a.attrs['data-video']
        id = video_url
        video_url = "https://www.imdb.com/videoplayer/" + video_url
        mp4Url = ''
        cnt_time = 0
        while mp4Url == '':
            video_html = self.isok_getUrlInfo(video_url)
            if video_html is None:
                self.log.error("Can't get this url:%s" % video_url)
                return
            if cnt_time > 10:
                self.log.error("Can't get this video:%s" % video_url)
                return
            video_soup = BeautifulSoup(video_html, "lxml")
            scripts = video_soup.find_all('script', attrs={"type": "text/javascript"})
            for script in scripts:
                text = script.get_text()
                text2 = script.string
                if text2 != None and text2 != '':
                    text = text + '\n' + text2
                urls = re.findall('"videoUrl":"(\S+?)"', text)
                urls2 = re.findall('\\\\"url\\\\":\\\\"(\S+?)\\\\"', text)
                urls.extend(urls2)
                for url in urls:
                    type = re.search('\.mp4\?', url)
                    # type2=re.search('pgv4ql', url)
                    if type:
                        mp4Url = url
                        break
                if mp4Url == '':
                    continue
                else:
                    break
            video_soup.decompose()
            cnt_time += 1
        # realUrl = re.search('(.+)u002Fvi(.+)\\\\u002F(.+?)\Z', mp4Url).groups()
        # realUrl = 'https://imdb-video.media-imdb.com/vi' + realUrl[1] + '/' + realUrl[2]
        self.log.info("video url:%s" % mp4Url)
        return mp4Url, id

    def getCompanies(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        productions = soup.find('h4', attrs={"id": "production"})
        item.companies = []
        tmp_comany = None
        if productions != None:
            productions = productions.next_sibling.next_sibling
            productions = productions.find_all('li')
            cur_type = "production"
            for production in productions:
                cur_company = [cur_type]
                tmp = production.get_text().strip().split()
                tmp = ' '.join(tmp)
                cur_company.append(tmp)
                cur_company.append(re.search("/(co\d+)", production.a.attrs['href']).group(1).strip())
                if tmp_comany is None:
                    tmp_comany = re.search("/(co\d+)", production.a.attrs['href']).group(1).strip()
                item.companies.append(cur_company)
        distributors = soup.find('h4', attrs={"id": "distributors"})
        if distributors != None:
            distributors = distributors.next_sibling.next_sibling
            distributors = distributors.find_all('li')
            cur_type = "distributor"
            for distributor in distributors:
                cur_company = [cur_type]
                tmp = distributor.get_text().strip().split()
                tmp = ' '.join(tmp)
                cur_company.append(tmp)
                cur_company.append(re.search("/(co\d+)", distributor.a.attrs['href']).group(1).strip())
                item.companies.append(cur_company)
        specialEffects = soup.find('h4', attrs={"id": "specialEffects"})
        if specialEffects != None:
            specialEffects = specialEffects.next_sibling.next_sibling
            specialEffects = specialEffects.find_all('li')
            cur_type = "specialEffect"
            for specialEffect in specialEffects:
                cur_company = [cur_type]
                tmp = specialEffect.get_text().strip().split()
                tmp = ' '.join(tmp)
                cur_company.append(tmp)
                cur_company.append(re.search("/(co\d+)", specialEffect.a.attrs['href']).group(1).strip())
                item.companies.append(cur_company)
        others = soup.find('h4', attrs={"id": "other"})
        if others != None:
            others = others.next_sibling.next_sibling
            others = others.find_all('li')
            cur_type = "other"
            for other in others:
                cur_company = [cur_type]
                tmp = other.get_text().strip().split()
                tmp = ' '.join(tmp)
                cur_company.append(tmp)
                cur_company.append(re.search("/(co\d+)", other.a.attrs['href']).group(1).strip())
                item.companies.append(cur_company)
        if tmp_comany is None:
            item.Gross = ""
            soup.decompose()
            return
        soup.decompose()
        # get Gross
        item.Gross = ""
        # base_search_url = "https://www.imdb.com/search/title/?companies=" + tmp_comany
        # start_pos = 1
        # start_search_url = base_search_url + "&start=" + str(start_pos)
        # tot_titles = 0
        # while True:
        #     htmlContent = self.isok_getUrlInfo(start_search_url)
        #     soup = BeautifulSoup(htmlContent, 'lxml')
        #     list = soup.find('div', attrs={"class": "lister-list"})
        #     if list is None:
        #         soup.decompose()
        #         break
        #     if tot_titles == 0:
        #         nums = soup.find("div", attrs={"class": "desc"}).find_all('span')
        #         nums = nums[0].get_text().strip()
        #         nums = re.findall('(\d+)', nums)
        #         if len(nums) >= 3:
        #             nums = ''.join(nums[2:])
        #         else:
        #             nums = ''.join(nums)
        #         tot_titles = int(nums)
        #     lists = list.find_all('div', class_="lister-item")
        #     for li in lists:
        #         li_content = li.find('div', attrs={"class": "lister-item-content"})
        #         title_id = li_content.h3.a.attrs["href"]
        #         title_id = re.search("/(tt\d+)", title_id).group(1).strip()
        #         if title_id != item.id:
        #             continue
        #         else:
        #             votes = li_content.find('p', attrs={"class": "sort-num_votes-visible"})
        #             if votes is None:
        #                 item.Gross = ""
        #                 break
        #             spans = votes.find_all("span")
        #             if len(spans) > 2:
        #                 value = spans[4].get_text().strip()
        #                 item.Gross = value
        #                 break
        #             else:
        #                 item.Gross = ""
        #                 break
        #     if item.Gross is not None:
        #         soup.decompose()
        #         break
        #     if start_pos > tot_titles:
        #         item.Gross = ""
        #         soup.decompose()
        #         break
        #     start_pos += len(lists)
        #     start_search_url = base_search_url + "&start=" + str(start_pos)
        #     soup.decompose()

    def getAwards(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        awards = soup.find_all('table', attrs={"class": "awards"})
        if awards != []:
            item.awards = []
            for award in awards:
                cur_award = []  # {"title":"","title_award_outcome_type":"","title_award_category":"","award_description":""}
                title = award.previous_sibling.previous_sibling.get_text().strip().split()
                title_id = re.search("/(ev\d+)", award.previous_sibling.previous_sibling.a.attrs["href"]).group(
                    1).strip()
                cur_award.append(' '.join(title))
                cur_award.append(title_id)
                trs = award.find_all('tr')
                title_award_outcome_type = None
                title_award_category = None
                infos = []
                for tr in trs:
                    td1 = tr.find("td", attrs={"class": "title_award_outcome"})
                    if td1 != None:
                        title_award_outcome_type = td1.b.get_text().strip()
                        title_award_category = td1.span.get_text().strip()
                    td2 = tr.find("td", attrs={"class": "award_description"})
                    award_description = td2.get_text().strip()
                    infos.append(
                        "title_award_outcome: " + title_award_outcome_type + ", award_category: " + title_award_category + ", describe: " + award_description)
                cur_award.append('||'.join(infos))
                item.awards.append(cur_award)
        soup.decompose()

    def getExternalreview(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        simpleList = soup.find("ul", attrs={"class": "simpleList"})
        if simpleList != None:
            item.externalreviews = []
            simpleList = simpleList.find_all("li")
            for li in simpleList:
                cur_review = [li.get_text().strip()]
                url = "https://www.imdb.com" + li.a.attrs['href']
                cur_review.append(url)
                item.externalreviews.append(cur_review)
        soup.decompose()

    def getParentalguide(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        item.parentalguides = []  # {"certificates":{"mpaa":"","list":[]},
        # "SexNudity":{"severity-vote":"","list":[]},
        # "ViolenceGore":{"severity-vote":"","list":[]},
        # "Profanity":{"severity-vote":"","list":[]},
        # "AlcoholDrugsSmoking":{"severity-vote":"","list":[]},
        # "Frightening_IntenseScenes":{"severity-vote":"","list":[]}}
        # 1、certificates
        certificates = soup.find('section', attrs={"id": "certificates"})
        if certificates != None:
            certificates_mpaa = certificates.find('tr', attrs={"id": "mpaa-rating"})
            certificates_list = certificates.find('tr', attrs={"id": "certifications-list"})
            mpaa = ""
            if certificates_mpaa != None:
                mpaa = "mpaa:" + certificates_mpaa.find_all('td')[1].get_text().strip()
            if certificates_list != None:
                certificates_list = certificates_list.find('ul', attrs={"class": "ipl-inline-list"})
                certificates_list = certificates_list.find_all("li")
                for li in certificates_list:
                    cur_guide = ["certificates", mpaa]
                    tmp = list(li.stripped_strings)
                    tmp = ' '.join(tmp)
                    cur_guide.append(tmp)
                    item.parentalguides.append(cur_guide)
        # 2、Sex & Nudity
        SexNudity = soup.find('section', attrs={"id": "advisory-nudity"})
        if SexNudity != None:
            severityVote = SexNudity.find('li', attrs={"class": "advisory-severity-vote"})
            severity_vote = ""
            if severityVote != None:
                tmp = severityVote.find("span", class_="ipl-status-pill")
                if tmp != None:
                    severity_vote = "severity_vote:" + tmp.get_text().strip()
            lists = SexNudity.find_all('li', attrs={"class": "ipl-zebra-list__item"})
            if lists != []:
                for li in lists:
                    cur_guide = ["Sex_Nudity", severity_vote]
                    cur_guide.append(list(li.stripped_strings)[0])
                    item.parentalguides.append(cur_guide)
        # 3、Violence & Gore
        ViolenceGore = soup.find('section', attrs={"id": "advisory-violence"})
        if ViolenceGore != None:
            severityVote = ViolenceGore.find('li', attrs={"class": "advisory-severity-vote"})
            severity_vote = ""
            if severityVote != None:
                tmp = severityVote.find("span", class_="ipl-status-pill")
                if tmp != None:
                    severity_vote = "severity_vote:" + tmp.get_text().strip()
            lists = ViolenceGore.find_all('li', attrs={"class": "ipl-zebra-list__item"})
            if lists != []:
                for li in lists:
                    cur_guide = ["Violence_Gore", severity_vote]
                    cur_guide.append(list(li.stripped_strings)[0])
                    item.parentalguides.append(cur_guide)
        # 4、Profanity
        Profanity = soup.find('section', attrs={"id": "advisory-profanity"})
        if Profanity != None:
            severityVote = Profanity.find('li', attrs={"class": "advisory-severity-vote"})
            severity_vote = ""
            if severityVote != None:
                tmp = severityVote.find("span", class_="ipl-status-pill")
                if tmp != None:
                    severity_vote = "severity_vote:" + tmp.get_text().strip()
            lists = Profanity.find_all('li', attrs={"class": "ipl-zebra-list__item"})
            if lists != []:
                for li in lists:
                    cur_guide = ["Profanity", severity_vote]
                    cur_guide.append(list(li.stripped_strings)[0])
                    item.parentalguides.append(cur_guide)
        # 5、Alcohol, Drugs & Smoking
        alcohol = soup.find('section', attrs={"id": "advisory-alcohol"})
        if alcohol != None:
            severityVote = alcohol.find('li', attrs={"class": "advisory-severity-vote"})
            severity_vote = ""
            if severityVote != None:
                tmp = severityVote.find("span", class_="ipl-status-pill")
                if tmp != None:
                    severity_vote = "severity_vote:" + tmp.get_text().strip()
            lists = alcohol.find_all('li', attrs={"class": "ipl-zebra-list__item"})
            if lists != []:
                for li in lists:
                    cur_guide = ["Alcohol_Drugs_Smoking", severity_vote]
                    cur_guide.append(list(li.stripped_strings)[0])
                    item.parentalguides.append(cur_guide)
        # 6、Frightening & Intense Scenes
        frightening = soup.find('section', attrs={"id": "advisory-frightening"})
        if frightening != None:
            severityVote = frightening.find('li', attrs={"class": "advisory-severity-vote"})
            severity_vote = ""
            if severityVote != None:
                tmp = severityVote.find("span", class_="ipl-status-pill")
                if tmp != None:
                    severity_vote = "severity_vote:" + tmp.get_text().strip()
            lists = frightening.find_all('li', attrs={"class": "ipl-zebra-list__item"})
            if lists != []:
                for li in lists:
                    cur_guide = ["Frightening_IntenseScenes", severity_vote]
                    cur_guide.append(list(li.stripped_strings)[0])
                    item.parentalguides.append(cur_guide)
        soup.decompose()

    def getFaqs(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        faqs = soup.find('section', attrs={"id": "faq-no-spoilers"})
        if faqs != None:
            faqs = faqs.find_all('li', attrs={"class": "ipl-zebra-list__item"})
            if faqs != []:
                item.faqs = []
                pool = ThreadPool(processes=self.THREADS)
                result = pool.map_async(self.faq2, faqs)
                pool.close()
                pool.join()
                result.wait()
                if result.ready():
                    if result.successful():
                        item.faqs = result.get()
        soup.decompose()

        """
        spoilers=soup.find('section',attrs={"id":"faq-spoilers"})
        if spoilers!=None:
            spoilers=spoilers.find_all('li',attrs={"class":"ipl-zebra-list__item"})
            if spoilers!=[]:
                for spoiler in spoilers:
                    cur_spoiler={"question":"","answer":""}
                    cur_spoiler["question"]=spoiler.find('div',attrs={"class":"faq-question-text"}).get_text().strip()
                    tmp=spoiler.find('div',class_="ipl-hideable-container").stripped_strings
                    cur_spoiler["answer"]='\n'.join(list(tmp)[:-1])
                    item.faqs.append(cur_spoiler)
        """

    def faq2(self, faq):
        cur_faq = {"question": "", "answer": ""}
        cur_faq["question"] = faq.find('div', attrs={"class": "faq-question-text"}).get_text().strip()
        tmp = faq.find('div', class_="ipl-hideable-container").stripped_strings
        cur_faq["answer"] = '\n'.join(list(tmp)[:-1])
        return cur_faq

    def getSoundtracks(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        soundtracks_content = soup.find('div', attrs={"id": "soundtracks_content"})
        if soundtracks_content != None:
            item.soundtracks = []
            soundtracks = soundtracks_content.find_all('div', class_="soundTrack")
            for soundtrack in soundtracks:
                item.soundtracks.append([soundtrack.get_text().strip()])
        soup.decompose()

    def getMovieconnections(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        connections_content = soup.find('div', attrs={"id": "connections_content"})
        if connections_content != None:
            connections_content = connections_content.find('div', attrs={"class": "list"})
            list_attrs = connections_content.attrs.keys()
            if 'id' in list_attrs:
                if connections_content.attrs['id'] == "no_content":
                    return
            tots = connections_content.children
            item.movieconnections = []
            con_type = ""
            for child in tots:
                if child != '\n':
                    if child.name == 'a':
                        con_type = child.attrs['id']
                    elif child.name == 'div':
                        cur_child = {"type": "%s" % con_type, "link_id": [], "link_name": [], "text": ""}
                        cur_child['text'] = child.get_text().strip().replace('\n', ' ')
                        links = child.find_all('a')
                        if links != []:
                            for cur_link in links:
                                cur_href = cur_link.attrs['href'].strip()
                                if "title" in cur_href:
                                    cur_child['link_id'].append(
                                        re.search("/(tt\d+)", cur_link.attrs['href'].strip()).group(1).strip())
                                else:
                                    cur_child['link_id'].append(
                                        re.search("/(nm\d+)", cur_link.attrs['href'].strip()).group(1).strip())
                                cur_child['link_name'].append(cur_link.get_text().strip())
                            cur_child['link_id'] = '||'.join(cur_child['link_id'])
                            cur_child['link_name'] = '||'.join(cur_child['link_name'])
                        else:
                            cur_child['link_id'] = ""
                            cur_child['link_name'] = ""
                        item.movieconnections.append(cur_child)
        soup.decompose()

    def getAlternateversions(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        alternateversions_content = soup.find('div', attrs={"id": "alternateversions_content"})
        if alternateversions_content != None:
            no_content = alternateversions_content.find('div', attrs={"id": "no_content"})
            if no_content == None:
                item.alternateversions = []
                alternateversions = alternateversions_content.find_all("div", attrs={"class": re.compile("soda")})
                for alternateversion in alternateversions:
                    item.alternateversions.append([alternateversion.get_text().strip()])
        soup.decompose()

    def getCrazycredits(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        crazycredits_content = soup.find('div', attrs={"id": "crazycredits_content"})
        if crazycredits_content != None:
            item.crazycredits = []
            crazycredits = crazycredits_content.find_all("div", attrs={"class": "sodatext"})
            for crazycredit in crazycredits:
                item.crazycredits.append([crazycredit.get_text().strip()])
        soup.decompose()

    def getQuotes(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        quotes_content = soup.find('div', attrs={"id": "quotes_content"})
        if quotes_content != None:
            item.quotes = []
            quotes = quotes_content.find_all("div", attrs={"class": "sodatext"})
            for quote in quotes:
                cur_quote_person_character = []
                cur_quote_person_id = []
                cur_quote_text = []
                pars = quote.find_all('p')
                for cur_p in pars:
                    alink = cur_p.find('a')
                    if alink != None:
                        cur_quote_person_id.append(re.search("/(nm\d+)", alink.attrs['href'].strip()).group(1).strip())
                        cur_quote_person_character.append(alink.get_text().strip())
                    else:
                        cur_quote_person_id.append("")
                        cur_quote_person_character.append("")
                    cur_quote_text.append(cur_p.get_text().strip().replace('\n', ' '))
                cur_quote_person_character = '||'.join(cur_quote_person_character)
                cur_quote_person_id = '||'.join(cur_quote_person_id)
                cur_quote_text = '||'.join(cur_quote_text)
                item.quotes.append([cur_quote_person_id, cur_quote_person_character, cur_quote_text])
        soup.decompose()

    def getGoofs(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        goofs_content = soup.find('div', attrs={"id": "goofs_content"})
        if goofs_content != None:
            item.goofs = []
            goofs = goofs_content.find_all("div", attrs={"class": "sodatext"})
            for goof in goofs:
                item.goofs.append([goof.get_text().strip()])
        soup.decompose()

    def getTrivia(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        trivia_content = soup.find('div', attrs={"id": "trivia_content"})
        if trivia_content != None:
            item.trivias = []
            trivias = trivia_content.find_all("div", attrs={"class": "sodatext"})
            for trivia in trivias:
                item.trivias.append([trivia.get_text().strip()])
        soup.decompose()

    def getLocations(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        locations = soup.find('section', attrs={"id": "filming_locations"})
        if locations != None:
            item.locations = []
            divs = locations.find_all('div', class_="soda")
            for div in divs:
                cur_loc = ["locations"]
                cur_loc.append(div.a.get_text().strip())
                item.locations.append(cur_loc)
        loc_date = soup.find('section', attrs={"id": "filming_dates"})
        if loc_date != None:
            if locations == None:
                item.locations = []
            lis = loc_date.find_all('li', attrs={"class": "ipl-zebra-list__item"})
            for li in lis:
                cur_loc = ["loc_date"]
                cur_loc.append(li.get_text().strip())
                item.locations.append(cur_loc)
        soup.decompose()

    def getTechnical(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        table = soup.find('table', class_="dataTable")
        if table != None:
            trs = table.find_all('tr')
            item.technicals = []
            for tr in trs:
                cur_tech = []  # {"label":"","value":""}
                tds = tr.find_all('td')
                cur_tech.append(tds[0].get_text().strip())
                tmp = tds[1].get_text().strip().split()
                cur_tech.append(' '.join(tmp))
                item.technicals.append(cur_tech)
        soup.decompose()

    def getReleaseinfo(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        releases = soup.find_all('tr', class_="release-date-item")
        if releases != []:
            item.releaseinfo = []
            for release in releases:
                cur_release = []  # {"country":"","date":"","attribute":""}
                tds = release.find_all('td')
                cur_release.append(tds[0].get_text().strip())
                cur_release.append(tds[1].get_text().strip())
                cur_release.append(tds[2].get_text().strip())
                item.releaseinfo.append(cur_release)
        soup.decompose()
        """
        akas=soup.find_all('tr', class_="aka-item")
        if akas!=[]:
            item.akas=[]
            for aka in akas:
                cur_aka=[]#{"country":"","title":""}
                tds=aka.find_all('td')
                cur_aka.append(tds[0].get_text().strip())
                cur_aka.append(tds[1].get_text().strip())
                item.akas.append(cur_aka)
        """

    def getKeywords(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        keywords = soup.find_all('div', attrs={"class": "sodatext"})
        item.keywords = []
        for keyword in keywords:
            item.keywords.append([keyword.get_text().strip()])
        soup.decompose()

    def getCredits(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        """
        #1、derectors
        directors = soup.find("h4", text=re.compile("Directed by"))
        if directors!=None:
            item.directors=[]
            directors = directors.next_sibling.next_sibling.find_all('td',attrs={"class":"name"})
            for director in directors:
                cur_director=[]
                cur_director.append(director.a.get_text().strip())
                cur_director.append(re.search("/name/(.+?)/", director.a.attrs['href']).group(1))
                item.directors.append(cur_director)
        #2、writers
        writers=soup.find(text=re.compile("Writing Credits"))
        if writers!=None:
            writers=writers.parent.next_sibling.next_sibling
            item.writers=[]
            writers=writers.find_all('tr')
            for writer in writers:
                cur_writer=[]
                tmp=writer.find('td',attrs={"class":"name"})
                if tmp==None:
                    continue
                else:
                    tmp=tmp.a
                cur_writer.append(tmp.get_text().strip())
                cur_writer.append(re.search("/name/(.+?)/", tmp.attrs['href']).group(1))
                tmp=writer.find('td',attrs={"class":"credit"})
                if tmp!=None:
                    cur_writer.append(tmp.get_text().strip())
                else:
                    cur_writer.append("")
                item.writers.append(cur_writer)
        """
        # 3、cast
        casts = soup.find('table', attrs={"class": "cast_list"})
        if casts != None:
            casts = casts.find_all('tr', attrs={"class": "odd"}) + casts.find_all('tr', attrs={"class": "even"})
            item.casts = []
            for cast in casts:
                cur_cast = []  # {"name":"","id":"","img":"","img_label","character":""}
                tmp = cast.find("td", attrs={"class": "primary_photo"})
                if tmp == None:
                    continue
                cur_cast.append(tmp.next_sibling.next_sibling.get_text().strip())
                tmp2 = tmp.next_sibling.next_sibling.a.attrs['href']
                cur_cast.append(re.search("/name/(.+?)/", tmp2).group(1))
                if "loadlate" in tmp.a.img.attrs.keys():
                    cur_cast.append(tmp.a.img.attrs["loadlate"])
                    cur_cast.append("1")
                else:
                    cur_cast.append(tmp.a.img.attrs["src"])
                    cur_cast.append("0")
                tmp = cast.find("td", attrs={"class": "character"})
                if tmp != None:
                    tmp2 = tmp.get_text().strip().split()
                    cur_cast.append(" ".join(tmp2))
                item.casts.append(cur_cast)
        soup.decompose()

    def getSum_Syn(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        sums = soup.find('ul', attrs={"class": "ipl-zebra-list", "id": "plot-summaries-content"})
        if sums != None:
            sums = sums.find_all('li', attrs={"class": "ipl-zebra-list__item"})
            item.Summaries = []
            for sum in sums:
                cur_sum = []  # {"text":"","author":""}
                cur_sum.append(sum.p.get_text().strip())
                author = sum.find('div', attrs={"class": "author-container"})
                if author != None:
                    cur_sum.append(author.a.get_text().strip())
                else:
                    cur_sum.append("")
                item.Summaries.append(cur_sum)
        syns = soup.find('ul', attrs={"class": "ipl-zebra-list", "id": "plot-synopsis-content"})
        if syns != None:
            syns = syns.find_all('li', attrs={"id": re.compile("synopsis")})
            item.Synopsis = []
            for syn in syns:
                text = syn.get_text().strip()
                item.Synopsis.append([text])
        soup.decompose()


class reviewIMDB(films_spider):
    def __init__(self, use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS):
        super().__init__(use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS)

    def spider(self, url):
        item = Item()
        item.id = re.search("/(tt\d+)", url).group(1).strip()
        reviews_csv = self.save_pre_dir + item.id + '/reviews.csv'
        if not self.is_repair:
            if os.path.exists(reviews_csv):
                self.log.info("%s 已经爬取" % url)
                return
        review_url = url + "/reviews"  # 评论
        self.getReviews(item, review_url)
        self.log.info('%s:review is over' % item.id)
        self.log.info("获取%s成功" % item.id)
        self.pipeline_save_url(item)

    def pipeline_save_url(self, item):
        item_dir = self.save_pre_dir + item.id
        if not os.path.exists(item_dir):
            os.makedirs(item_dir)
        # 1、评论[["rating","title","name","date","text"]]
        self.save_reviews_csv(item, item_dir)
        self.log.info("%s写入文件成功" % (item.id))

    def save_reviews_csv(self, item, item_dir):
        reviews_csv = item_dir + '/reviews.csv'
        with open(reviews_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["rating", "title", "name", "ID", "date", "text", "action"])
            if item.reviews != None:
                writer.writerows(item.reviews)

    def getReviews(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        if htmlContent is None:
            self.log.error("Can't get this url: %s" % url)
            if os.path.exists(self.save_pre_dir + item.id):
                delete_dir(self.save_pre_dir + item.id)
            return
        soup = BeautifulSoup(htmlContent, 'lxml')
        button = soup.find('button', attrs={"id": "load-more-trigger"})

        if button != None:
            self.revirews_selenium(item, url)
            return

        reviews = soup.find_all('div', class_="lister-item")
        if reviews != []:
            self.log.info("%s tot reviews:%d" % (item.id, len(reviews)))
            pool = ThreadPool(processes=self.THREADS)
            res = pool.map_async(self.getEachReview, reviews)
            pool.close()
            pool.join()
            res.wait()
            if res.ready():
                if res.successful():
                    item.reviews = res.get()
        soup.decompose()

    def revirews_selenium(self, item, url):
        self.log.info("selenium:%s" % url)
        driver = webdriver.Chrome(chrome_options=self.chrome_options, )#seleniumwire_options=self.seleniumwire_options
        # tot_reviews=None
        while True:
            try:
                driver.get(url)
                htmlcontent = driver.page_source
                soup_tmp = BeautifulSoup(htmlcontent, 'lxml')
                nums = soup_tmp.find('div', attrs={"class": "lister"}).find("div", attrs={"class": "header"}).find(
                    'div').get_text().strip()
                nums = re.findall('(\d+)', nums)
                if len(nums) >= 3:
                    nums = ''.join(nums[2:])
                else:
                    nums = ''.join(nums)
                tot_reviews = int(nums)
                soup_tmp.decompose()
                break
            except Exception as e:
                self.log.error("selenium: 获取driver失败，重试中(%s)" % e)
        self.log.info("%s has %d reviews" % (url, tot_reviews))
        cnt_try = 0
        while cnt_try <= 10:
            try:
                while True:
                    next_cn = driver.find_element(By.ID,'load-more-trigger')#driver.find_element_by_id('load-more-trigger')
                    driver.execute_script("arguments[0].scrollIntoView();", next_cn)
                    next_cn.click()
                    driver.implicitly_wait(5)
            except Exception as e:
                self.log.info("%s  selenium over!(%s)" % (url, e))
            htmlcontent = driver.page_source
            soup = BeautifulSoup(htmlcontent, 'lxml')
            reviews = soup.find('div', attrs={"class": "lister-list"}).find_all('div', class_="lister-item")
            self.log.info("%s get reviews:%d/%d" % (item.id, len(reviews), tot_reviews))
            load_all = soup.find("div", attrs={"class": "lister"}).find('div',
                                                                        attrs={"class": re.compile("loaded-all")})
            if load_all is not None:
                self.log.info("%s  selenium over!" % url)
                soup.decompose()
                break
            soup.decompose()
            cnt_try += 1

        if reviews != []:
            pool = ThreadPool(processes=self.THREADS)
            res = pool.map_async(self.getEachReview, reviews)
            pool.close()
            pool.join()
            res.wait()
            if res.ready():
                if res.successful():
                    item.reviews = res.get()
        driver.delete_all_cookies()
        driver.close()
        driver.quit()

    def getEachReview(self, review):
        cur_review = []  # {"rating":"","title":"","name":"","ID":"","date":"","text":"","action":""}
        rating = review.find('span', attrs={"class": "rating-other-user-rating"})
        if rating != None:
            cur_review.append(rating.get_text().strip())
        else:
            cur_review.append("")
        title = review.find('a', attrs={"class": "title"})
        if title != None:
            cur_review.append(title.get_text().strip())
        else:
            cur_review.append("")
        name_date = review.find('div', attrs={"class": "display-name-date"})
        if name_date != None:
            cur_review.append(name_date.a.get_text().strip())
            cur_review.append(re.search("/(ur\d+)", name_date.a.attrs['href'].strip()).group(1).strip())
            cur_review.append(name_date.find('span', attrs={"class": "review-date"}).get_text().strip())
        else:
            cur_review.append("")
            cur_review.append("")
            cur_review.append("")
        content = review.find('div', attrs={"class": "content"}).find('div', class_="text")
        if content != None:
            cur_review.append(content.get_text().strip())
        else:
            cur_review.append("")
        action = review.find('div', attrs={"class": "content"}).find('div', class_="actions")
        if action != None:
            cur_review.append(action.contents[0].strip())
        else:
            cur_review.append("")
        return cur_review


class imgTags_IMDB(films_spider):
    def __init__(self, use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS):
        super().__init__(use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS)

    def start_spider(self):
        tot_urls = len(self.urls)
        self.log.info("total ids:%d" % tot_urls)
        epochs = tot_urls // (self.THREADS * 2)
        if tot_urls % (self.THREADS * 2):
            epochs += 1
        start_epoch = 0
        if os.path.exists(self.start_epoch_file):
            with open(self.start_epoch_file, "r", encoding="utf8") as fi:
                lines = fi.readlines()
                start_epoch = int(lines[0]) + 1

        self.start_time = time.time()
        self.end_time = self.start_time
        for i in range(start_epoch, epochs):
            self.sum_dict = {
                "large_img_info_samples": 0,
                "large_img_info_attributes": 0,
                "data_TagsForLargeImg_crawled": 0,
            }
            start_id = i * (self.THREADS * 2)
            end_id = (i + 1) * (self.THREADS * 2)
            self.items = []
            self.log.error("start %d:%d--%d" % (i, start_id, end_id))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.spider, self.urls[start_id:end_id])
            pool.close()
            pool.join()
            with open(self.start_epoch_file, "w", encoding="utf8") as fi:
                fi.write(str(i))
            if i % 2 == 0:
                shutil.copy(self.start_epoch_file, self.start_epoch_file + '.bak.txt')

            news_json_str=json.dumps(self.sum_dict, indent=4)
            self.log.info("news_sum_info: " + news_json_str)
            json_file = "./data_TagsForLargeImg_sum_tt_info.json"
            json_bak_file = "./data_TagsForLargeImg_sum_tt_info_bak.json"
            if i % 2 == 0:
                shutil.copy(json_file, json_bak_file)
            with open(json_file, 'r') as load_f:
                load_dict = json.load(load_f)
            for cur_key in load_dict.keys():
                load_dict[cur_key] += self.sum_dict[cur_key]
                self.sum_dict[cur_key] = 0
            json_str = json.dumps(load_dict, indent=4)
            self.log.info("sum_info: " + json_str)
            with open(json_file, "w") as f:
                f.write(json_str)

        self.end_time = time.time()
        total_time = self.end_time - self.start_time
        self.log.info('爬取数据信息及链接完成，总用时： {:.0f}m {:.0f}s'.format(total_time // 60, total_time % 60))

    def spider(self, url):
        item = Item()
        item.id = re.search("/(tt\d+)", url).group(1).strip()
        imgtag_csv = self.save_pre_dir + item.id + '/large_img_info.csv'
        if not self.is_repair:
            if os.path.exists(imgtag_csv):
                self.log.info("%s 已经爬取" % url)
                return
        img_url = url + "/mediaindex"  # 电影相关图片
        html2 = self.isok_getUrlInfo(img_url)
        if html2 is None:
            self.log.info("Can't get this url: %s" % img_url)
            if os.path.exists(self.save_pre_dir + item.id):
                delete_dir(self.save_pre_dir + item.id)
            return
        soup2 = BeautifulSoup(html2, 'lxml')
        leftright = soup2.find('div', attrs={"id": "media_index_content", "class": "header"})
        if leftright != None:
            photos = leftright.find('div', attrs={"id": "left", "class": "desc"})
            if photos != None:
                photos = photos.get_text().strip()
                nums = re.findall('(\d+)', photos)
                if len(nums) >= 3:
                    nums = ''.join(nums[2:])
                else:
                    nums = ''.join(nums)
                nums = int(nums)
                self.getAllimgsinfo(item, img_url, nums)
        soup2.decompose()
        self.log.info('%s:imgtags is over' % item.id)
        self.log.info("获取%s成功" % item.id)
        self.pipeline_save_url(item)

    def getAllimgsinfo(self, item, img_url, nums):
        self.log.info("%s有%s张图片" % (item.id, nums))
        pages = nums // self.PER_PAGE_OF_IMG
        if nums % self.PER_PAGE_OF_IMG:
            pages += 1
        page_url = []
        orlList = []
        for i in range(1, pages + 1):
            page_url.append(img_url + '?page=' + str(i))
        pool = ThreadPool(processes=self.THREADS)
        res = pool.map_async(self.get_img_oriList, page_url)
        pool.close()
        pool.join()
        res.wait()
        if res.ready():
            if res.successful():
                orlLists = res.get()
                for i in orlLists:
                    orlList.extend(i)
        self.log.info("%s tot imgs:%d" % (item.id, len(orlList)))

        pool = ThreadPool(processes=self.THREADS)
        res = pool.map_async(self.get_ori_imgs_info, orlList)
        pool.close()
        pool.join()
        res.wait()
        if res.ready():
            if res.successful():
                tmp = res.get()
                self.log.info("%d img tags get for %d imgs" % (len(tmp), len(orlList)))
                item.large_img_tag = tmp

    def get_img_oriList(self, cur_url):
        htmlContent = self.isok_getUrlInfo(cur_url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        imgs = soup.find('div', attrs={"class": "media_index_thumb_list", "id": "media_index_thumbnail_grid"})
        imgs = imgs.find_all('img')
        orlList = []
        for img in imgs:
            img_ori_url = "https://www.imdb.com" + img.parent.attrs['href']
            orlList.append(img_ori_url)
        soup.decompose()
        return orlList

    def get_ori_imgs_info(self, img_url):
        self.log.info("selenium:%s" % img_url)
        driver = webdriver.Chrome(chrome_options=self.chrome_options, )#seleniumwire_options=self.seleniumwire_options
        try_cnt = 0
        info_list=[]
        while try_cnt <= 10:
            while True:
                try:
                    driver.get(img_url)
                    driver.implicitly_wait(3)
                    # driver = webdriver.Firefox(firefox_options=chrome_options)
                    break
                except Exception as e:
                    self.log.error("selenium: 获取driver失败，重试中(%s)" % e)

            try:
                button = driver.find_elements(By.CSS_SELECTOR,'button[aria-label="Open"]') #driver.find_elements_by_css_selector('button[aria-label="Open"]')
                if len(button) > 0:
                    driver.execute_script("arguments[0].scrollIntoView();", button[0])
                    button[0].click()
            except Exception as e:
                self.log.error(e)
            try:
                htmlcontent = driver.page_source
                soup = BeautifulSoup(htmlcontent, 'lxml')
                img_ori_url = soup.find('meta', attrs={"property": "og:image"})
                img_id = re.search("/(rm\d+)", img_url).group(1).strip()
                if img_ori_url is None:
                    self.log.error("%s 获取地址出错" % img_url)
                    driver.delete_all_cookies()
                    driver.close()
                    driver.quit()
                    soup.decompose()
                    return ["", "", "", "", ""]
                img_ori_url = img_ori_url.attrs['content']
                title = soup.find('title').get_text().strip()
                describe = soup.find('meta', attrs={"name": "description"}).attrs["content"].strip()
                content = soup.find('div', attrs={"class": "item-metadata"})
                # content=soup.find('div',attrs={"data-testid":"media-sheet"})
                content = content.find_all('div')
                info_list = [img_id, img_ori_url, title, describe]
                info_other = []
                for div in content:
                    type = div.find('strong')
                    if type == None:
                        continue
                    strong_text = type.get_text().strip()
                    info = div.get_text().strip()
                    info_other.append(info)
                    if re.search("People", strong_text):
                        people_string = "People_ID: "
                        people_ids = []
                        links = div.find_all('a')
                        for link in links:
                            link_url = link.attrs['href'].strip()
                            link_id = re.search("/(nm\d+)", link_url).group(1).strip()
                            people_ids.append(link_id)
                        people_string += ','.join(people_ids)
                        info_other.append(people_string)
                info_list.append('||'.join(info_other))
                soup.decompose()
                break
            except Exception as e:
                self.log.error("selenium: 获取信息失败，重试中(%s)" % e)
                try:
                    htmlcontent = driver.page_source
                    soup = BeautifulSoup(htmlcontent, 'lxml')
                    img_ori_url = soup.find('meta', attrs={"property": "og:image"})
                    img_id = re.search("/(rm\d+)", img_url).group(1).strip()
                    if img_ori_url is None:
                        self.log.error("%s 获取地址出错" % img_url)
                        driver.delete_all_cookies()
                        driver.close()
                        driver.quit()
                        soup.decompose()
                        return ["", "", "", "", ""]
                    img_ori_url = img_ori_url.attrs['content']
                    content = soup.find('div', attrs={"data-testid": "media-sheet"})
                    content = content.contents[0]
                    chs = content.contents
                    title = chs[0].contents[0].get_text().strip()
                    other = chs[1]
                    other_chs = other.contents
                    describe = other_chs[0].get_text().strip()
                    info_list = [img_id, img_ori_url, title, describe]
                    info_other = []
                    for div in other_chs[2].contents:
                        spans = div.contents
                        cur_t, cur_c = None, None
                        if len(spans) < 2:
                            cur_t = div.attrs['data-testid'].strip() + ': '
                            cur_c = div.get_text().strip()
                        else:
                            cur_t = spans[0].get_text().strip() + ': '
                            cur_c = spans[1].get_text().strip()
                        info_other.append(cur_t + cur_c)
                        if re.search("People", cur_t):
                            people_string = "People_ID: "
                            people_ids = []
                            links = spans[1].find_all('a')
                            for link in links:
                                link_url = link.attrs['href'].strip()
                                link_id = re.search("/(nm\d+)", link_url).group(1).strip()
                                people_ids.append(link_id)
                            people_string += ','.join(people_ids)
                            info_other.append(people_string)
                    info_list.append('||'.join(info_other))
                    soup.decompose()
                    break
                except Exception as e:
                    self.log.error(e)
                    try_cnt += 1

        if try_cnt > 10:
            self.log.error("获取原图信息出错：%s" % img_url)
            driver.delete_all_cookies()
            driver.close()
            driver.quit()
            return ["", "", "", "", ""]
        self.log.info("%s:selenium over!" % img_url)
        driver.delete_all_cookies()
        driver.close()
        driver.quit()
        return info_list


    def pipeline_save_url(self, item):
        item_dir = self.save_pre_dir + item.id
        if not os.path.exists(item_dir):
            os.makedirs(item_dir)
        self.save_imgtags_csv(item, item_dir)
        self.log.info("%s写入文件成功" % (item.id))

    def save_imgtags_csv(self, item, item_dir):
        large_img_info_csv = item_dir + '/large_img_info.csv'
        with open(large_img_info_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["ID", "url", "title", 'decribe', 'info'])
            if item.large_img_tag != None:
                writer.writerows(item.large_img_tag)
                self.sum_dict["large_img_info_samples"] += len(item.large_img_tag)
                self.sum_dict["large_img_info_attributes"] += len(item.large_img_tag) * 5
                self.sum_dict["data_TagsForLargeImg_crawled"] += 1


class newsIMDB(films_spider):
    def __init__(self, use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS):
        super().__init__(use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS)

    def spider(self, url):
        item = Item()
        item.id = re.search("/(tt\d+)", url).group(1).strip()
        news_csv = self.save_pre_dir + item.id + '/news.csv'
        if not self.is_repair:
            if os.path.exists(news_csv):
                self.log.info("%s 已经爬取" % news_csv)
                return
        news_url = url + "/news"  # 新闻
        self.getNews(item, news_url)
        self.log.info("获取%s成功" % item.id)
        self.pipeline_save_url(item)

    def pipeline_save_url(self, item):
        item_dir = self.save_pre_dir + item.id
        if not os.path.exists(item_dir):
            os.makedirs(item_dir)
        # 1、新闻#[title,url,date,author,source,source_url,img_url,content]
        self.save_news_csv(item, item_dir)
        self.log.info("%s写入文件成功" % (item.id))

    def save_news_csv(self, item, item_dir):
        news_csv = item_dir + '/news.csv'
        with open(news_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["title", "url", "date", "author", "source", "source_url", "img_url", "content"])
            if item.news != None:
                writer.writerows(item.news)

    def getNews(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        if htmlContent is None:
            self.log.error("Can't get this url: %s" % url)
            if os.path.exists(self.save_pre_dir + item.id):
                delete_dir(self.save_pre_dir + item.id)
            return
        soup = BeautifulSoup(htmlContent, 'lxml')
        button = soup.find('button', attrs={"id": "news-load-more"})
        if button != None:
            self.news_selenium(item, url)
            soup.decompose()
            return

        newses = soup.find_all('article', attrs={"class": re.compile("news-article")})
        if newses != []:
            self.log.info("%s tot news:%d" % (item.id, len(newses)))
            pool = ThreadPool(processes=self.THREADS)
            res = pool.map_async(self.getEachNews, newses)
            pool.close()
            pool.join()
            res.wait()
            if res.ready():
                if res.successful():
                    item.news = res.get()
        soup.decompose()

    def news_selenium(self, item, url):
        self.log.info("selenium:%s" % url)
        get_tot_flag = False
        driver = webdriver.Chrome(chrome_options=self.chrome_options, )  # executable_path='./chromedriver',seleniumwire_options=self.seleniumwire_options
        while True:
            if get_tot_flag == True:
                break
            while True:
                try:
                    driver.get(url)
                    driver.implicitly_wait(1)
                    break
                except:
                    self.log.error("selenium: 获取driver失败，重试中")

            while True:
                try:
                    while True:
                        next =driver.find_element(By.ID,'news-load-more') #driver.find_element_by_id('news-load-more')
                        driver.execute_script("arguments[0].scrollIntoView();", next)
                        next.click()
                        driver.implicitly_wait(3)
                except Exception as e:
                    self.log.error(e)
                    htmlcontent = driver.page_source
                    soup = BeautifulSoup(htmlcontent, 'lxml')
                    load_all = soup.find("div", attrs={"id": "main"}).find('div',
                                                                           attrs={"class": re.compile("loaded-all")})
                    if load_all is not None:
                        get_tot_flag = True
                        self.log.info("%s  selenium over!" % url)
                        soup.decompose()
                        break
                    soup.decompose()

        htmlcontent = driver.page_source
        soup = BeautifulSoup(htmlcontent, 'lxml')
        newses = soup.find_all('article', attrs={"class": re.compile("news-article")})
        if newses != []:
            self.log.info("%s tot news:%d" % (item.id, len(newses)))
            pool = ThreadPool(processes=self.THREADS)
            res = pool.map_async(self.getEachNews, newses)
            pool.close()
            pool.join()
            res.wait()
            if res.ready():
                if res.successful():
                    item.news = res.get()
        driver.delete_all_cookies()
        driver.close()
        driver.quit()
        soup.decompose()

    def getEachNews(self, news):
        cur_news = []  # [title,url,date,author,source,source_url,img_url,content]
        header = news.find('header')
        if header != None:
            title = header.find('h2', attrs={"class": "news-article__title"})
            if title != None:
                cur_news.append(title.get_text().strip())
                Url = title.find('a')
                if Url != None:
                    Url = Url.attrs['href']
                    cur_news.append(Url)
                else:
                    cur_news.append("")
            else:
                cur_news.append("")
                cur_news.append("")
            date = header.find('li', attrs={"class": re.compile("news-article__date")})
            if date != None:
                cur_news.append(date.get_text().strip())
            else:
                cur_news.append("")
            author = header.find('li', attrs={"class": re.compile("news-article__author")})
            if author != None:
                cur_news.append(author.get_text().strip())
            else:
                cur_news.append("")
            source = header.find('li', attrs={"class": re.compile("news-article__source")})
            if source != None:
                cur_news.append(source.get_text().strip())
                alink = source.find('a')
                if alink != None:
                    cur_news.append(alink.attrs['href'].strip())
                else:
                    cur_news.append("")
            else:
                cur_news.append("")
                cur_news.append("")
        else:
            cur_news.append("")
            cur_news.append("")
            cur_news.append("")
            cur_news.append("")
            cur_news.append("")
            cur_news.append("")
        img = news.find("img", attrs={"class": "news-article__image"})
        if img != None:
            cur_news.append(img.attrs['src'].strip())
        else:
            cur_news.append("")
        content = news.find("div", attrs={"class": "news-article__content"})
        if content != None:
            cur_news.append(content.get_text().strip())
        else:
            cur_news.append("")
        return cur_news


class nmIMDB(films_spider):
    def __init__(self, use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS):
        super().__init__(use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS)
        self.PER_PAGE_OF_HIST = 50

    def start_spider(self):
        tot_urls = len(self.urls)
        self.log.info("total ids:%d" % tot_urls)
        epochs = tot_urls // (self.THREADS * 2)
        if tot_urls % (self.THREADS * 2):
            epochs += 1
        start_epoch = 0
        if os.path.exists(self.start_epoch_file):
            with open(self.start_epoch_file, "r", encoding="utf8") as fi:
                lines = fi.readlines()
                start_epoch = int(lines[0]) + 1

        self.start_time = time.time()
        self.end_time = self.start_time
        for i in range(start_epoch, epochs):
            self.sum_dict = {
                "base_samples": 0,
                "base_attributes": 0,
                "imgs_samples": 0,
                "imgs_attributes": 0,
                "videos_samples": 0,
                "videos_attributes": 0,
                "hists_samples": 0,
                "hists_attributes": 0,
                "bio_samples": 0,
                "bio_attributes": 0,
                "awards_samples": 0,
                "awards_attributes": 0,
                "otherworks_samples": 0,
                "otherworks_attributes": 0,
                "publicity_samples": 0,
                "publicity_attributes": 0,
                "external_sites_samples": 0,
                "external_sites_attributes": 0,
                "data_nms_crawled": 0,
            }
            start_id = i * (self.THREADS * 2)
            end_id = (i + 1) * (self.THREADS * 2)
            self.items = []
            self.log.error("start %d:%d--%d" % (i, start_id, end_id))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.spider, self.urls[start_id:end_id])
            pool.close()
            pool.join()
            with open(self.start_epoch_file, "w", encoding="utf8") as fi:
                fi.write(str(i))
            if i % 2 == 0:
                shutil.copy(self.start_epoch_file, self.start_epoch_file + '.bak.txt')

            json_file = "./data_nms_sum_nm_info.json"
            json_bak_file = "./data_nms_sum_nm_info_bak.json"
            if i % 2 == 0:
                shutil.copy(json_file, json_bak_file)
            with open(json_file, 'r') as load_f:
                load_dict = json.load(load_f)
            for cur_key in load_dict.keys():
                load_dict[cur_key] += self.sum_dict[cur_key]
                self.sum_dict[cur_key] = 0
            json_str = json.dumps(load_dict, indent=4)
            self.log.info("sum_info: " + json_str)
            with open(json_file, "w") as f:
                f.write(json_str)

        self.end_time = time.time()
        total_time = self.end_time - self.start_time
        self.log.info('爬取数据信息及链接完成，总用时： {:.0f}m {:.0f}s'.format(total_time // 60, total_time % 60))

    def spider(self, url):
        item = Item()
        item.id = re.search("/(nm\d+)", url).group(1).strip()
        base_csv = self.save_pre_dir + item.id + '/base.csv'
        if not self.is_repair:
            if os.path.exists(base_csv):
                self.log.info("%s 已经爬取" % url)
                return

        htmlContent = self.isok_getUrlInfo(url)
        if htmlContent is None:
            self.log.error("Can't get this url:%s" % url)
            if os.path.exists(self.save_pre_dir + item.id):
                delete_dir(self.save_pre_dir + item.id)
            return
        soup = BeautifulSoup(htmlContent, 'lxml')

        header = soup.find('h1', attrs={"class": "header"})
        if header != None:
            nmName = header.find('span', attrs={"class": "itemprop"})
        else:
            nmName = None
        while nmName == None:
            htmlContent = self.isok_getUrlInfo(url)
            soup = BeautifulSoup(htmlContent, 'lxml')
            header = soup.find('h1', attrs={"class": "header"})
            if header != None:
                nmName = header.find('span', attrs={"class": "itemprop"})
            else:
                nmName = None
        item.nmName = nmName.get_text().strip()
        jobs = soup.find('div', attrs={"id": "name-job-categories", "class": "infobar"})
        if jobs != None:
            jobs = jobs.find_all('a')
            item.jobs = '|'.join([i.get_text().strip() for i in jobs])
        else:
            item.jobs = ''
        born = soup.find('div', attrs={"id": "name-born-info"})
        if born != None:
            born = born.strings
            item.born = ''.join([i.strip() for i in born])
        else:
            item.born = ''
        soup.decompose()

        if self.is_repair:
            # 5、awards
            award_url = url + '/awards'
            self.getAwards(item, award_url)

        if not self.is_repair:
            # 2、photos
            pic_url = url + '/mediaindex'
            html2 = self.isok_getUrlInfo(pic_url)
            if html2 is None:
                self.log.error("Can't get this url: %s" % pic_url)
                return
            soup2 = BeautifulSoup(html2, 'lxml')
            desc = soup2.find('div', attrs={"id": "left", "class": "desc"})
            if desc != None:
                nums = desc.get_text().strip()
                nums = re.findall('(\d+)', nums)
                if len(nums) >= 3:
                    nums = ''.join(nums[2:])
                else:
                    nums = ''.join(nums)
                nums = int(nums)
                self.getAllimgs(item, pic_url, nums)
            soup2.decompose()
            # 3、videos
            video_url = url + '/videogallery'
            html3 = self.isok_getUrlInfo(video_url)
            if html3 is None:
                self.log.error("Can't get this url: %s" % pic_url)
                return
            soup3 = BeautifulSoup(html3, 'lxml')
            desc = soup3.find('span', attrs={"id": "vg-left"})
            if desc != None:
                video_nums = desc.get_text().strip()
                video_nums = re.findall('(\d+)', video_nums)
                if len(video_nums) >= 3:
                    video_nums = ''.join(video_nums[2:])
                else:
                    video_nums = ''.join(video_nums)
                video_nums = int(video_nums)
                self.getAllvideos(item, video_url, video_nums)
            soup3.decompose()
            # 1、Filmography
            hist_url = 'https://www.imdb.com/filmosearch/?sort=year&explore=title_type&role=' + item.id
            self.get_hist(item, hist_url)
            # 4、bio
            bio_url = url + '/bio'
            self.get_bio(item, bio_url)
            # 5、awards
            award_url = url + '/awards'
            self.getAwards(item, award_url)
            # 6、otherworks
            otherworks_url = url + '/otherworks'
            self.get_otherworks(item, otherworks_url)
            # 7、publicity
            publicity_url = url + '/publicity'
            self.get_publicity(item, publicity_url)
            # 8、external_sites
            external_sites_url = url + '/externalsites'
            self.get_external_sites(item, external_sites_url)

        self.log.info("获取%s成功" % item.id)
        self.pipeline_save_url(item)

    def pipeline_save_url(self, item):
        item_dir = self.save_pre_dir + item.id
        if not os.path.exists(item_dir):
            os.makedirs(item_dir)
        # 1、base
        info_csv = item_dir + '/base.csv'
        with open(info_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(['id', 'name', 'jobs', 'born'])
            writer.writerow([item.id, item.nmName, item.jobs, item.born])
            self.sum_dict["base_samples"] += 1
            self.sum_dict["base_attributes"] += 4
            self.sum_dict["data_nms_crawled"] += 1
        if self.is_repair:
            # 6、awards
            self.save_awards_info(item, item_dir)

        if not self.is_repair:
            # 2、imgs
            self.save_img_info(item, item_dir)
            # 3、videos
            self.save_video_info(item, item_dir)
            # 4、hist
            self.save_hist_csv(item, item_dir)
            # 5、bio
            self.save_bio_csv(item, item_dir)
            # 6、awards
            self.save_awards_info(item, item_dir)
            # 7、otherworks
            self.save_otherworks_csv(item, item_dir)
            # 8、publicity
            self.save_publicity_csv(item, item_dir)
            # 9、external_sites
            self.save_external_sites_csv(item, item_dir)
        self.log.info("%s写入文件成功" % (item.id))

    def save_img_info(self, item, item_dir):
        imgs_csv = item_dir + '/imgs.csv'
        with open(imgs_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["small", "large", "ID", "ori"])
            if item.imgs != None:
                self.log.info("save: %d small pics %d,large pics %d,ori pics %d" % (
                    len(item.imgs["ID"]), len(item.imgs["small"]), len(item.imgs["large"]), len(item.imgs["ori"])))
                all_img = list(zip(item.imgs["small"], item.imgs["large"], item.imgs["ID"], item.imgs["ori"]))
                writer.writerows(all_img)
                self.sum_dict["imgs_samples"] += len(all_img)
                self.sum_dict["imgs_attributes"] += len(all_img) * 4

    def save_video_info(self, item, item_dir):
        videos_csv = item_dir + '/videos.csv'
        with open(videos_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["url", "ID"])
            if item.videos != None:
                self.log.info("save : %d videos %d" % (len(item.videos['ID']), len(item.videos['url'])))
                all_video = list(zip(item.videos["url"], item.videos["ID"]))
                writer.writerows(all_video)
                self.sum_dict["videos_samples"] += len(all_video)
                self.sum_dict["videos_attributes"] += len(all_video) * 2

    def save_awards_info(self, item, item_dir):
        # ["title", "year","ID", "award_outcome","award_category","award_descriptions"]
        awards_csv = item_dir + '/awards.csv'
        with open(awards_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["title", "year", "ID", "award_outcome", "award_category", "award_descriptions"])
            if item.awards != None:
                writer.writerows(item.awards)
                self.sum_dict["awards_samples"] += len(item.awards)
                self.sum_dict["awards_attributes"] += len(item.awards) * 6

    def getAwards(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        awards = soup.find_all('table', attrs={"class": "awards"})
        if awards != []:
            item.awards = []
            for award in awards:
                title = award.previous_sibling.previous_sibling.get_text().strip().split()
                title = ' '.join(title)
                trs = award.find_all('tr')
                award_year = None
                award_id = None
                award_outcome = None
                award_category = None
                for cur_tr in trs:
                    # ["title", "year","ID", "award_outcome","award_category","award_descriptions"]
                    if cur_tr.find("td", attrs={"class": "award_year"}) is not None:
                        award_year = cur_tr.find("td", attrs={"class": "award_year"}).get_text().strip()
                        award_id = cur_tr.find("td", attrs={"class": "award_year"}).a.attrs['href'].strip()
                        award_id = re.search("/(ev\d+)", award_id).group(1).strip()
                    if cur_tr.find("td", attrs={"class": "award_outcome"}) is not None:
                        award_outcome_node = cur_tr.find("td", attrs={"class": "award_outcome"})
                        award_outcome = award_outcome_node.b.get_text().strip()
                        award_category = award_outcome_node.find("span",
                                                                 attrs={"class": "award_category"}).get_text().strip()
                    award_descriptions = cur_tr.find("td", attrs={"class": "award_description"}).get_text().strip()
                    cur_award = [title, award_year, award_id, award_outcome, award_category, award_descriptions]
                    item.awards.append(cur_award)
        soup.decompose()

    def save_hist_csv(self, item, item_dir):
        hists_csv = item_dir + '/hists.csv'
        with open(hists_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["id", "url", "title"])
            if item.hists != None:
                writer.writerows(item.hists)
                self.sum_dict["hists_samples"] += len(item.hists)
                self.sum_dict["hists_attributes"] += len(item.hists) * 3

    def save_bio_csv(self, item, item_dir):
        bio_csv = item_dir + '/bio.csv'
        with open(bio_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["cate", "label", "value"])
            if item.bios != None:
                writer.writerows(item.bios)
                self.sum_dict["bio_samples"] += len(item.bios)
                self.sum_dict["bio_attributes"] += len(item.bios) * 3

    def save_otherworks_csv(self, item, item_dir):
        otherworks_csv = item_dir + '/otherworks.csv'
        with open(otherworks_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["text"])
            if item.otherwork != None:
                writer.writerows(item.otherwork)
                self.sum_dict["otherworks_samples"] += len(item.otherwork)
                self.sum_dict["otherworks_attributes"] += len(item.otherwork) * 1

    def save_publicity_csv(self, item, item_dir):
        publicity_csv = item_dir + '/publicity.csv'
        with open(publicity_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["cate", "label", "value"])
            if item.publicity != None:
                writer.writerows(item.publicity)
                self.sum_dict["publicity_samples"] += len(item.publicity)
                self.sum_dict["publicity_attributes"] += len(item.publicity) * 3

    def save_external_sites_csv(self, item, item_dir):
        external_sites_csv = item_dir + '/external_sites.csv'
        with open(external_sites_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(['cate', "external_url", "text"])
            if item.external_sites != None:
                writer.writerows(item.external_sites)
                self.sum_dict["external_sites_samples"] += len(item.external_sites)
                self.sum_dict["external_sites_attributes"] += len(item.external_sites) * 3

    def get_hist(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        nums = soup.find("div", attrs={"class": "desc"})
        if nums != None:
            nums = nums.find(text=re.compile('title')).strip()
            nums = ''.join(re.findall('(\d+)', nums))
            nums = int(nums)
            self.log.info("%s hists:%d" % (item.id, nums))
            pages = nums // self.PER_PAGE_OF_HIST
            if nums % self.PER_PAGE_OF_HIST:
                pages += 1
            page_urls = [url + '&page=' + str(i) for i in range(1, pages + 1)]
            item.hists = []
            pool = ThreadPool(processes=self.THREADS)
            res = pool.map_async(self.get_cur_hist, page_urls)
            pool.close()
            pool.join()
            res.wait()
            if res.ready():
                if res.successful():
                    tmp = res.get()
                    for tmp_list in tmp:
                        item.hists.extend(tmp_list)
                    self.log.info('%s get hist len:%d' % (item.id, len(item.hists)))
        soup.decompose()

    def get_cur_hist(self, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        list = soup.find('div', attrs={"class": "lister-list"})
        lis = list.find_all('div', attrs={"class": "lister-item-content"})
        ans = []
        for li in lis:
            h3 = li.find('h3')
            title = h3.find_all('a')[0]
            title_id = re.search('(tt\d+)', title.attrs['href']).group(1)
            ans.append([title_id, 'https://www.imdb.com' + title.attrs['href'], title.get_text().strip()])
        soup.decompose()
        return ans

    def get_bio(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        item.bios = []
        overview = soup.find('table', attrs={"id": "overviewTable"})
        if overview != None:
            trs = overview.find_all('tr')
            for tr in trs:
                tds = tr.find_all('td')
                label = tds[0].get_text().strip()
                value = tds[1].strings
                value = ' '.join([i.strip() for i in value])
                item.bios.append(['overview', label, value])
        mini_bio = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Mini Bio"))
        if mini_bio != None:
            mini_bio = mini_bio.next_sibling.next_sibling
            ps = mini_bio.find_all('p')
            value = ps[0].get_text().strip()
            label = ''
            if len(ps) == 2:
                label = ps[1].strings
                label = ''.join([i.strip() for i in label])
            item.bios.append(['mini_bio', label, value])
        spouse = soup.find('table', attrs={"id": "tableSpouses"})
        if spouse != None:
            trs = spouse.find_all('tr')
            for tr in trs:
                tds = tr.find_all('td')
                label = tds[0].get_text().strip()
                value = tds[1].strings
                value = ''.join([i.strip() for i in value])
                item.bios.append(['spouse', label, value])
        trade_mark = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Trade Mark"))
        if trade_mark != None:
            trade_mark = trade_mark.next_sibling.next_sibling
            while trade_mark.name == 'div':
                value = trade_mark.get_text().strip()
                item.bios.append(['trade_mark', '', value])
                trade_mark = trade_mark.next_sibling.next_sibling
        trivia = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Trivia"))
        if trivia != None:
            trivia = trivia.next_sibling.next_sibling
            while trivia.name == 'div':
                value = trivia.get_text().strip()
                item.bios.append(['trivia', '', value])
                trivia = trivia.next_sibling.next_sibling
                if trivia == None:
                    break
        personal_quotes = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Personal Quotes"))
        if personal_quotes != None:
            personal_quotes = personal_quotes.next_sibling.next_sibling
            while personal_quotes.name == 'div':
                value = personal_quotes.get_text().strip()
                item.bios.append(['personal_quotes', '', value])
                personal_quotes = personal_quotes.next_sibling.next_sibling
        salary = soup.find('table', attrs={"id": "salariesTable"})
        if salary != None:
            trs = salary.find_all('tr')
            for tr in trs:
                tds = tr.find_all('td')
                label = tds[0].get_text().strip()
                value = tds[1].strings
                value = ''.join([i.strip() for i in value])
                item.bios.append(['spouse', label, value])
        soup.decompose()

    def get_otherworks(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        ul = soup.find('ul', attrs={"class": "ipl-zebra-list"})
        if ul != None:
            lis = ul.find_all("li")
            item.otherwork = []
            for li in lis:
                item.otherwork.append([li.get_text().strip()])
        soup.decompose()

    def get_publicity(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        item.publicity = []
        Print_Biographies = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Print Biographies"))
        if Print_Biographies != None:
            Print_Biographies = Print_Biographies.next_sibling.next_sibling
            if Print_Biographies.name == 'table':
                trs = Print_Biographies.find_all('tr')
                for tr in trs:
                    text = tr.strings
                    text = ''.join([i.strip() for i in text])
                    item.publicity.append(['Print_Biographies', '', text])
        Film_Biographies = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Film Biographies"))
        if Film_Biographies != None:
            Film_Biographies = Film_Biographies.next_sibling.next_sibling
            if Film_Biographies.name == 'ul':
                lis = Film_Biographies.find_all('li')
                for li in lis:
                    text = li.strings
                    text = ''.join([i.strip() for i in text])
                    item.publicity.append(['Film_Biographies', '', text])
        Interviews = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Interviews"))
        if Interviews != None:
            Interviews = Interviews.next_sibling.next_sibling
            if Interviews.name == 'table':
                trs = Interviews.find_all('tr')
                for tr in trs:
                    tds = tr.find_all('td')
                    label = tds[0].get_text().strip()
                    text = tds[1].get_text().strip()
                    item.publicity.append(['Interviews', label, text])
        Articles = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Articles"))
        if Articles != None:
            Articles = Articles.next_sibling.next_sibling
            if Articles.name == 'table':
                trs = Articles.find_all('tr')
                for tr in trs:
                    tds = tr.find_all('td')
                    label = tds[0].get_text().strip()
                    text = tds[1].get_text().strip()
                    item.publicity.append(['Articles', label, text])
        Pictorials = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Pictorials"))
        if Pictorials != None:
            Pictorials = Pictorials.next_sibling.next_sibling
            if Pictorials.name == 'table':
                trs = Pictorials.find_all('tr')
                for tr in trs:
                    tds = tr.find_all('td')
                    label = tds[0].get_text().strip()
                    text = tds[1].get_text().strip()
                    item.publicity.append(['Pictorials', label, text])
        Magazine_Covers = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Magazine Covers"))
        if Magazine_Covers != None:
            Magazine_Covers = Magazine_Covers.next_sibling.next_sibling
            if Magazine_Covers.name == 'table':
                trs = Magazine_Covers.find_all('tr')
                for tr in trs:
                    tds = tr.find_all('td')
                    label = tds[0].strings
                    label = ''.join([i.strip() for i in label])
                    text = tds[1].get_text().strip()
                    item.publicity.append(['Magazine_Covers', label, text])
        Portrayals = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Portrayals"))
        if Portrayals != None:
            Portrayals = Portrayals.next_sibling.next_sibling
            if Portrayals.name == 'ul':
                lis = Portrayals.find_all('li')
                for li in lis:
                    text = li.strings
                    text = ''.join([i.strip() for i in text])
                    item.publicity.append(['Portrayals', '', text])
        soup.decompose()

    def get_external_sites(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        item.external_sites = []
        Official_Sites = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Official Sites"))
        if Official_Sites != None:
            Official_Sites = Official_Sites.next_sibling.next_sibling
            if Official_Sites.name == 'ul':
                lis = Official_Sites.find_all('li')
                for li in lis:
                    text = li.a.get_text().strip()
                    external_url = 'https://www.imdb.com' + li.a.attrs['href']
                    item.external_sites.append(['Official_Sites', external_url, text])
        Miscellaneous_Sites = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Miscellaneous Sites"))
        if Miscellaneous_Sites != None:
            Miscellaneous_Sites = Miscellaneous_Sites.next_sibling.next_sibling
            if Miscellaneous_Sites.name == 'ul':
                lis = Miscellaneous_Sites.find_all('li')
                for li in lis:
                    text = li.a.get_text().strip()
                    external_url = 'https://www.imdb.com' + li.a.attrs['href']
                    item.external_sites.append(['Miscellaneous_Sites', external_url, text])
        Photographs = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Photographs"))
        if Photographs != None:
            Photographs = Photographs.next_sibling.next_sibling
            if Photographs.name == 'ul':
                lis = Photographs.find_all('li')
                for li in lis:
                    text = li.a.get_text().strip()
                    external_url = 'https://www.imdb.com' + li.a.attrs['href']
                    item.external_sites.append(['Photographs', external_url, text])
        Video_Clips = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Video Clips"))
        if Video_Clips != None:
            Video_Clips = Video_Clips.next_sibling.next_sibling
            if Video_Clips.name == 'ul':
                lis = Video_Clips.find_all('li')
                for li in lis:
                    text = li.a.get_text().strip()
                    external_url = 'https://www.imdb.com' + li.a.attrs['href']
                    item.external_sites.append(['Video_Clips', external_url, text])
        Sound_Clips = soup.find('h4', attrs={"class": "li_group"}, text=re.compile("Sound Clips"))
        if Sound_Clips != None:
            Sound_Clips = Sound_Clips.next_sibling.next_sibling
            if Sound_Clips.name == 'ul':
                lis = Sound_Clips.find_all('li')
                for li in lis:
                    text = li.a.get_text().strip()
                    external_url = 'https://www.imdb.com' + li.a.attrs['href']
                    item.external_sites.append(['Video_Clips', external_url, text])
        soup.decompose()


class eventIMDB(films_spider):
    def __init__(self, use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS):
        super().__init__(use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS)

    def spider(self, url):
        item = Item()
        item.id = re.search("/(ev\d+)", url).group(1).strip()
        if not self.is_repair:
            event_csv = self.save_pre_dir + item.id + '/event.csv'
            if os.path.exists(event_csv):
                self.log.info("%s 已经爬取" % url)
                return
        event_url = url
        self.getEvent(item, event_url)
        self.log.info("获取%s成功" % item.id)
        self.pipeline_save_url(item)

    def pipeline_save_url(self, item):
        item_dir = self.save_pre_dir + item.id
        if not os.path.exists(item_dir):
            os.makedirs(item_dir)

        self.save_event_csv(item, item_dir)

        base_csv = item_dir + '/base.csv'
        with open(base_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["ID", "name", "sub_name", "year"])
            writer.writerow([item.id, item.name, item.sub_name, item.year])
        self.log.info("%s写入文件成功" % (item.id))

    def save_event_csv(self, item, item_dir):
        event_csv = item_dir + '/event.csv'
        with open(event_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            # [award_name,cate_name,isWinner,img_url,primary_Names,primary_IDs,secondary_Names,secondary_IDs,notes_detail]
            writer.writerow(
                ["award_name", "cate_name", "isWinner", 'img_url', 'primary_Names', 'primary_IDs', 'secondary_Names',
                 'secondary_IDs', 'notes_detail'])
            if item.event != None:
                writer.writerows(item.event)

    def getEvent(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        if htmlContent is None:
            self.log.error("Can't get this url: %s" % url)
            return
        self.log.info("selenium:%s" % url)
        driver = webdriver.Chrome(chrome_options=self.chrome_options, )#seleniumwire_options=self.seleniumwire_options
        while True:
            try:
                driver.get(url)
                driver.implicitly_wait(5)
                # driver = webdriver.Firefox(firefox_options=chrome_options)
                break
            except Exception as e:
                print("selenium: 获取driver失败，重试中(%s)" % e)

        html = driver.page_source
        soup = BeautifulSoup(html, 'lxml')
        if soup.find("div", attrs={"class": "event-header__title"}) is not None:
            item.name = soup.find("div", attrs={"class": "event-header__title"}).get_text().strip()
            sub_name = soup.find("div", attrs={"class": "event-header__subtitle"})
            if sub_name is not None:
                item.sub_name = sub_name.get_text().strip()
            else:
                item.sub_name = ""
            year = soup.find("div", attrs={"class": "event-year-header__year"})
            if year is not None:
                nums = re.search("(\d+)", year.get_text().strip()).group(1).strip()
                item.year = nums
            else:
                item.year = ""

            award_list = soup.find("div", attrs={"class": "event-widgets__award-list"})
            if award_list is None:
                driver.delete_all_cookies()
                driver.close()
                driver.quit()
                soup.decompose()
                return
            lis = award_list.find_all("div", attrs={"class": "event-widgets__award"})
            if lis != []:
                item.event = []
                self.log.info("%s has %d awards" % (url, len(lis)))
                for li in lis:
                    award_name = li.find("div", attrs={"class": "event-widgets__award-name"}).get_text().strip()
                    cates = li.find_all("div", attrs={"class": "event-widgets__award-category"})

                    for cate in cates:
                        cate_name = ""
                        if cate.find("div", attrs={"class": "event-widgets__award-category-name"}) is not None:
                            cate_name = cate.find("div", attrs={
                                "class": "event-widgets__award-category-name"}).get_text().strip()
                        nominations = cate.find_all("div", attrs={"class": "event-widgets__award-nomination"})
                        for nomination in nominations:
                            isWinner = "not Winner"
                            img = nomination.find("div", attrs={"class": "event-widgets__nominee-image"})
                            img_url = img.find("img").attrs["src"]
                            primary_IDs, secondary_IDs = [], []
                            primary_Names, secondary_Names = [], []
                            notes_detail = ""
                            details = nomination.find("div", attrs={"class": "event-widgets__nomination-details"})
                            if details.find("div", attrs={"class": "event-widgets__winner-badge"}):
                                isWinner = "Winner"
                            nominees = details.find("div", attrs={"class": "event-widgets__nominees"})
                            primary_nominees = nominees.find("div", attrs={"class": "event-widgets__primary-nominees"})
                            primary_spans = primary_nominees.contents
                            for cur_span in primary_spans:
                                cur_name = cur_span.find("span", attrs={
                                    "class": "event-widgets__nominee-name"}).get_text().strip()
                                cur_id = cur_span.find("span", attrs={"class": "event-widgets__nominee-name"}).a.attrs[
                                    'href']
                                if re.search('name', cur_id) is not None:
                                    cur_id = re.search("/(nm\d+)", cur_id).group(1).strip()
                                elif re.search('company', cur_id) is not None:
                                    cur_id = re.search("/(co\d+)", cur_id).group(1).strip()
                                else:
                                    cur_id = re.search("/(tt\d+)", cur_id).group(1).strip()

                                primary_Names.append(cur_name)
                                primary_IDs.append(cur_id)
                            primary_Names = ",".join(primary_Names)
                            primary_IDs = ",".join(primary_IDs)
                            secondary_nominees = nominees.find("div",
                                                               attrs={"class": "event-widgets__secondary-nominees"})
                            secondary_spans = secondary_nominees.contents
                            for cur_span in secondary_spans:
                                cur_name = cur_span.find("span", attrs={
                                    "class": "event-widgets__nominee-name"}).get_text().strip()
                                cur_id = cur_span.find("span", attrs={"class": "event-widgets__nominee-name"}).a.attrs[
                                    'href']
                                if re.search('name', cur_id) is not None:
                                    cur_id = re.search("/(nm\d+)", cur_id).group(1).strip()
                                elif re.search('company', cur_id) is not None:
                                    cur_id = re.search("/(co\d+)", cur_id).group(1).strip()
                                else:
                                    cur_id = re.search("/(tt\d+)", cur_id).group(1).strip()
                                secondary_Names.append(cur_name)
                                secondary_IDs.append(cur_id)
                            secondary_Names = ",".join(secondary_Names)
                            secondary_IDs = ",".join(secondary_IDs)
                            notes = details.find("div", attrs={"class": "event-widgets__nomination-notes"})
                            if notes != None:
                                notes_detail = notes.get_text().strip()
                            item.event.append(
                                [award_name, cate_name, isWinner, img_url, primary_Names, primary_IDs, secondary_Names,
                                 secondary_IDs, notes_detail])
        else:
            scripts = soup.find_all("script", type="text/javascript")
            cur_scipt = None
            for tmp in scripts:
                if re.search("IMDbReactWidgets.NomineesWidget.push", tmp.text):
                    cur_scipt = tmp
                    break
            # cur_scipt=cur_scipt.text
            # cur_scipt=cur_scipt.split("\n")
            # print(cur_scipt.text)
            js = re.search('(\{"nomineesWidgetModel".*\})', cur_scipt.text).group(1).strip()
            awards_scipt = json.loads(js)
            awards_scipt = awards_scipt['nomineesWidgetModel']
            # print(awards_scipt)
            item.name = awards_scipt['eventEditionSummary']['eventName']
            item.sub_name = ""
            item.year = awards_scipt['eventEditionSummary']['year']
            awards = awards_scipt['eventEditionSummary']['awards']
            item.event = []
            self.log.info("%s has %d awards" % (url, len(awards)))
            for award in awards:
                award_name = award['awardName']
                cates = award['categories']
                for cate in cates:
                    cate_name = ""
                    if cate['categoryName'] is not None:
                        cate_name = cate['categoryName']
                    nominations = cate['nominations']
                    for nomination in nominations:
                        isWinner = "not Winner"
                        img_url = ""
                        primary_IDs, secondary_IDs = [], []
                        primary_Names, secondary_Names = [], []
                        notes_detail = ""
                        if nomination['isWinner']:
                            isWinner = "Winner"
                        if nomination['notes'] is not None:
                            notes_detail = nomination['notes']
                        primaryNominees = nomination['primaryNominees']
                        for Nominee in primaryNominees:
                            if Nominee['imageUrl'] is not None:
                                img_url = Nominee['imageUrl']
                            primary_Names.append(Nominee['name'])
                            primary_IDs.append(Nominee['const'])
                        secondaryNominees = nomination['secondaryNominees']
                        for Nominee in secondaryNominees:
                            secondary_Names.append(Nominee['name'])
                            secondary_IDs.append(Nominee['const'])
                        primary_Names = ",".join(primary_Names)
                        primary_IDs = ",".join(primary_IDs)
                        secondary_Names = ",".join(secondary_Names)
                        secondary_IDs = ",".join(secondary_IDs)
                        item.event.append(
                            [award_name, cate_name, isWinner, img_url, primary_Names, primary_IDs, secondary_Names,
                             secondary_IDs, notes_detail])

        driver.delete_all_cookies()
        driver.close()
        driver.quit()
        soup.decompose()


class downloads(object):
    def __init__(self, type, log_file, THREADS):
        self.log = MyLog(log_file)
        self.THREADS = THREADS
        self.SEP = '\t'
        if type == "nm":
            self.base_dir = dirs['IMDB_DOWNLOAD_NMS_DIR']
            if not os.path.exists(self.base_dir):
                os.makedirs(self.base_dir)
            self.titles = self.get_files(dirs['IMDB_NM_DIR'], "nm\d+")
        else:
            self.base_dir = dirs['IMDB_DOWNLOAD_FILMS_DIR']
            # self.base_dir =dirs["REQUIRE_FILM_DIR"]
            if not os.path.exists(self.base_dir):
                os.makedirs(self.base_dir)
            self.titles = self.get_files(dirs['IMDB_FILMS_DIR'], "tt\d+")
            # self.titles = self.get_files(dirs['REQUIRE_FILM_DIR'], "tt\d+")

        self.base_pic_dir = self.base_dir + 'pics/'
        # self.base_pic_dir = self.base_dir
        # self.pic_remain_cnt=0
        self.base_video_dir = self.base_dir + 'videos/'
        if not os.path.exists(self.base_pic_dir):
            os.makedirs(self.base_pic_dir)
        if not os.path.exists(self.base_video_dir):
            os.makedirs(self.base_video_dir)
        self.pic_remain_csv = self.base_pic_dir + 'remain.csv'
        self.video_remain_csv = self.base_video_dir + 'remain.csv'
        self.remain_pic_nums, self.remain_video_nums = 0, 0
        if not os.path.exists(self.pic_remain_csv):
            with open(self.pic_remain_csv, 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.SEP)
                writer.writerow(["ID", "download_url", "save_file"])
        if not os.path.exists(self.video_remain_csv):
            with open(self.video_remain_csv, 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.SEP)
                writer.writerow(["ID", "download_url", "save_file"])

        tot_ids = len(self.titles)
        epochs = tot_ids // (self.THREADS * 4)
        if tot_ids % (self.THREADS * 4):
            epochs += 1
        start_epoch = 0
        for i in range(start_epoch, epochs):
            start_id = i * (self.THREADS * 4)
            end_id = (i + 1) * (self.THREADS * 4)
            self.items = []
            self.log.error("start %d(%d):%d--%d" % (i, epochs, start_id, end_id))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.download_pics, self.titles[start_id:end_id])
            pool.close()
            pool.join()
        self.log.info("下载图片完成！")
        # self.log.info("剩余图片：%d"%self.pic_remain_cnt)
        # start_epoch = 0
        # for i in range(start_epoch, epochs):
        #     start_id = i * (self.THREADS * 4)
        #     end_id = (i + 1) * (self.THREADS * 4)
        #     self.items = []
        #     self.log.error("start %d(%d):%d--%d"%(i,epochs,start_id,end_id))
        #     pool = ThreadPool(processes=self.THREADS)
        #     pool.map(self.download_videos, self.titles[start_id:end_id])
        #     pool.close()
        #     pool.join()
        # self.log.info("下载视频完成！")

    def download_pics(self, id):
        try:
            pic_file = id + '/imgs.csv'
            # pic_file = id + '/casts.csv'
            data = pd.read_csv(pic_file, sep=self.SEP)
            smalls = data['small'].values.tolist()
            # smalls = data['img'].values.tolist()
            larges = data['large'].values.tolist()
            oris = []
            # for i in larges:
            for i in smalls:
                tmp = i.split('.')
                ori_img = tmp[:3] + tmp[-1:]
                ori_img = '.'.join(ori_img)
                oris.append(ori_img)
            ids = data['ID'].values.tolist()
            # ids =data['id'].values.tolist()

            tots = list(zip([id] * len(smalls), ids, smalls, oris, larges))
            # tots=list(zip([id]*len(smalls),ids,smalls,oris))

            # pic_id_dir = id + '/casts_imgs/'
            # if not os.path.exists(pic_id_dir):
            #     os.makedirs(pic_id_dir)

            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.download_pics_per, tots)
            pool.close()
            pool.join()

        except Exception as e:
            self.log.info(e)
            self.log.error("%s :获取csv文件失败" % id)

    def download_pics_per(self, cur_img):
        id = cur_img[0]
        cur_id = cur_img[1]
        small_pic = cur_img[2]
        ori_pic = cur_img[3]
        large_pic = cur_img[4]
        pic_id_dir = self.base_pic_dir + cur_id + '/'
        # print(pic_id_dir)
        # pic_id_dir = id + '/casts_imgs/'
        if not os.path.exists(pic_id_dir):
            os.makedirs(pic_id_dir)
        # pic_id_dir = pic_id_dir + cur_id + '/'
        # if not os.path.exists(pic_id_dir):
        #     os.makedirs(pic_id_dir)
        small_file = pic_id_dir + cur_id + "_small.jpg"
        large_file = pic_id_dir + cur_id + "_large.jpg"
        ori_file = pic_id_dir + cur_id + "_ori.jpg"
        if not os.path.exists(small_file):
            # self.pic_remain_cnt+=1
            self.download_for_ipg(cur_id, small_pic, small_file)
        if not os.path.exists(large_file):
            # self.pic_remain_cnt += 1
            self.download_for_ipg(cur_id, large_pic, large_file)
        if not os.path.exists(ori_file):
            # self.pic_remain_cnt += 1
            self.download_for_ipg(cur_id, ori_pic, ori_file)

    def download_videos(self, id):
        try:
            video_file = id + '/videos.csv'
            data = pd.read_csv(video_file, sep=self.SEP)
            videos = data['url'].values.tolist()
            ids = data['ID'].values.tolist()
            for pos in range(len(ids)):
                cur_id = ids[pos]
                video_id_dir = self.base_video_dir + cur_id + '/'
                if not os.path.exists(video_id_dir):
                    os.makedirs(video_id_dir)
                video_file = video_id_dir + cur_id + ".mp4"
                if not os.path.exists(video_file):
                    self.download_for_video(cur_id, videos[pos], video_file)
        except Exception as e:
            self.log.info("e")
            self.log.error("%s :获取csv文件失败" % id)

    def download_for_ipg(self, id, url, file):
        cnt = 0
        while cnt <= 10:
            try:
                r = requests.get(url, timeout=(10, 10), stream=True)
                with open(file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=128):
                        f.write(chunk)
                # urlretrieve(url, file)
                self.log.info("获取图片%s 成功" % url)
                return
            except:
                cnt += 1
        self.log.error("获取图片%s 失败" % url)
        with open(self.pic_remain_csv, 'a+', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.SEP)
            writer.writerow([id, url, file])
        self.remain_pic_nums += 1

    def download_for_video(self, id, url, file):
        try:
            r = requests.get(url, stream=True)
            with open(file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            self.log.info("获取视频%s 成功" % url)
        except:
            self.log.error("获取视频%s 失败" % url)
            with open(self.video_remain_csv, 'a+', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.SEP)
                writer.writerow([id, url, file])
            self.remain_video_nums += 1

    def get_files(self, dir, pattern):
        files = []
        count = 0
        for fn in os.listdir(dir):
            if re.match(pattern, fn) and os.path.isdir(dir + fn):
                count += 1
                files.append(dir + fn)
        self.log.info("get titles:%d" % count)
        return files


class downloads_videos(object):
    def __init__(self, type, log_file, THREADS):
        self.log = MyLog(log_file)
        self.THREADS = THREADS
        self.SEP = '\t'
        if type == "nm":
            self.base_dir = dirs['IMDB_DOWNLOAD_NMS_DIR']
            if not os.path.exists(self.base_dir):
                os.makedirs(self.base_dir)
            self.titles = self.get_files(dirs['IMDB_NM_DIR'], "nm\d+")
        else:
            self.base_dir = dirs['IMDB_DOWNLOAD_FILMS_DIR']
            if not os.path.exists(self.base_dir):
                os.makedirs(self.base_dir)
            self.titles = self.get_files(dirs['IMDB_FILMS_DIR'], "tt\d+")

        self.base_pic_dir = self.base_dir + 'pics/'
        self.base_video_dir = self.base_dir + 'videos/'
        if not os.path.exists(self.base_pic_dir):
            os.makedirs(self.base_pic_dir)
        if not os.path.exists(self.base_video_dir):
            os.makedirs(self.base_video_dir)
        self.pic_remain_csv = self.base_pic_dir + 'remain.csv'
        self.video_remain_csv = self.base_video_dir + 'remain.csv'
        self.remain_pic_nums, self.remain_video_nums = 0, 0
        if not os.path.exists(self.pic_remain_csv):
            with open(self.pic_remain_csv, 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.SEP)
                writer.writerow(["ID", "download_url", "save_file"])
        if not os.path.exists(self.video_remain_csv):
            with open(self.video_remain_csv, 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.SEP)
                writer.writerow(["ID", "download_url", "save_file"])

        tot_ids = len(self.titles)
        epochs = tot_ids // (self.THREADS * 4)
        if tot_ids % (self.THREADS * 4):
            epochs += 1
        # start_epoch = 0
        # for i in range(start_epoch,epochs):
        #     start_id=i*(self.THREADS*4)
        #     end_id=(i+1)*(self.THREADS*4)
        #     self.items=[]
        #     self.log.error("start %d(%d):%d--%d"%(i,epochs,start_id,end_id))
        #     pool = ThreadPool(processes=self.THREADS)
        #     pool.map(self.download_pics, self.titles[start_id:end_id])
        #     pool.close()
        #     pool.join()
        # self.log.info("下载图片完成！")
        start_epoch = 0
        for i in range(start_epoch, epochs):
            start_id = i * (self.THREADS * 4)
            end_id = (i + 1) * (self.THREADS * 4)
            self.items = []
            self.log.error("start %d(%d):%d--%d" % (i, epochs, start_id, end_id))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.download_videos, self.titles[start_id:end_id])
            pool.close()
            pool.join()
        self.log.info("下载视频完成！")

    def download_pics(self, id):
        try:
            pic_file = id + '/imgs.csv'
            data = pd.read_csv(pic_file, sep=self.SEP)
            smalls = data['small'].values.tolist()
            larges = data['large'].values.tolist()
            oris = []
            for i in larges:
                tmp = i.split('.')
                ori_img = tmp[:3] + tmp[-1:]
                ori_img = '.'.join(ori_img)
                oris.append(ori_img)
            ids = data['ID'].values.tolist()
            for pos in range(len(ids)):
                cur_id = ids[pos]
                pic_id_dir = self.base_pic_dir + cur_id + '/'
                if not os.path.exists(pic_id_dir):
                    os.makedirs(pic_id_dir)
                small_file = pic_id_dir + cur_id + "_small.jpg"
                large_file = pic_id_dir + cur_id + "_large.jpg"
                ori_file = pic_id_dir + cur_id + "_ori.jpg"
                if not os.path.exists(small_file):
                    self.download_for_ipg(cur_id, smalls[pos], small_file)
                if not os.path.exists(large_file):
                    self.download_for_ipg(cur_id, larges[pos], large_file)
                if not os.path.exists(ori_file):
                    self.download_for_ipg(cur_id, oris[pos], ori_file)
        except Exception as e:
            self.log.info("e")
            self.log.error("%s :获取csv文件失败" % id)

    def download_videos(self, id):
        try:
            video_file = id + '/videos.csv'
            data = pd.read_csv(video_file, sep=self.SEP)
            videos = data['url'].values.tolist()
            ids = data['ID'].values.tolist()
            for pos in range(len(ids)):
                cur_id = ids[pos]
                video_id_dir = self.base_video_dir + cur_id + '/'
                if not os.path.exists(video_id_dir):
                    os.makedirs(video_id_dir)
                video_file = video_id_dir + cur_id + ".mp4"
                if not os.path.exists(video_file):
                    self.download_for_video(cur_id, videos[pos], video_file)
        except Exception as e:
            self.log.info("e")
            self.log.error("%s :获取csv文件失败" % id)

    def download_for_ipg(self, id, url, file):
        try:
            r = requests.get(url, stream=True)
            with open(file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=128):
                    f.write(chunk)
            self.log.info("获取图片%s 成功" % url)
        except:
            self.log.error("获取图片%s 失败" % url)
            with open(self.pic_remain_csv, 'a+', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.SEP)
                writer.writerow([id, url, file])
            self.remain_pic_nums += 1

    def download_for_video(self, id, url, file):
        try:
            r = requests.get(url, stream=True)
            with open(file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            self.log.info("获取视频%s 成功" % url)
        except:
            self.log.error("获取视频%s 失败" % url)
            with open(self.video_remain_csv, 'a+', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.SEP)
                writer.writerow([id, url, file])
            self.remain_video_nums += 1

    def get_files(self, dir, pattern):
        files = []
        count = 0
        for fn in os.listdir(dir):
            if re.match(pattern, fn) and os.path.isdir(dir + fn):
                count += 1
                files.append(dir + fn)
        self.log.info("get titles:%d" % count)
        return files


class repairImg(object):
    def __init__(self, type, THREADS):
        self.log = MyLog("repair_img.log")
        self.THREADS = THREADS
        self.SEP = '\t'
        if type == "nm":
            self.base_dir = dirs['IMDB_DOWNLOAD_NMS_DIR']
            if not os.path.exists(self.base_dir):
                os.makedirs(self.base_dir)
            self.titles = self.get_files(dirs['IMDB_NM_DIR'], "nm\d+")
        else:
            self.base_dir = dirs['IMDB_DOWNLOAD_FILMS_DIR']
            if not os.path.exists(self.base_dir):
                os.makedirs(self.base_dir)
            self.titles = self.get_files(dirs['IMDB_FILMS_DIR'], "tt\d+")

        self.base_pic_dir = self.base_dir + 'pics/'
        self.base_video_dir = self.base_dir + 'videos/'
        if not os.path.exists(self.base_pic_dir):
            os.makedirs(self.base_pic_dir)
        if not os.path.exists(self.base_video_dir):
            os.makedirs(self.base_video_dir)
        self.pic_remain_csv = self.base_pic_dir + 'remain.csv'
        self.video_remain_csv = self.base_video_dir + 'remain.csv'
        self.remain_pic_nums, self.remain_video_nums = 0, 0
        if not os.path.exists(self.pic_remain_csv):
            with open(self.pic_remain_csv, 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.SEP)
                writer.writerow(["ID", "download_url", "save_file"])
        if not os.path.exists(self.video_remain_csv):
            with open(self.video_remain_csv, 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.SEP)
                writer.writerow(["ID", "download_url", "save_file"])

        tot_ids = len(self.titles)
        epochs = tot_ids // (self.THREADS * 4)
        if tot_ids % (self.THREADS * 4):
            epochs += 1
        start_epoch = 0
        for i in range(start_epoch, epochs):
            start_id = i * (self.THREADS * 4)
            end_id = (i + 1) * (self.THREADS * 4)
            self.items = []
            self.log.error("start %d:%d--%d" % (i, start_id, end_id))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.repair_pics, self.titles[start_id:end_id])
            pool.close()
            pool.join()
        self.log.info("repair图片完成！")

    def repair_pics(self, id):
        pic_file = id + '/imgs.csv'
        data = pd.read_csv(pic_file, sep=self.SEP)
        larges = data['large'].values.tolist()
        oris = []
        for i in larges:
            tmp = i.split('.')
            ori_img = tmp[:3] + tmp[-1:]
            ori_img = '.'.join(ori_img)
            oris.append(ori_img)
        data['ori'] = oris
        data.to_csv(pic_file, sep=self.SEP, index=False)

    def get_files(self, dir, pattern):
        files = []
        count = 0
        for fn in os.listdir(dir):
            if re.match(pattern, fn) and os.path.isdir(dir + fn):
                count += 1
                files.append(dir + fn)
        self.log.info("get titles:%d" % count)
        return files


class getComanies(baseSpider):
    def __init__(self, save_dir, log_file, THREADS, use_csv=None):
        super().__init__()
        self.save_dir = save_dir
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)
        self.log = MyLog(log_file)
        self.THREADS = THREADS
        self.SEP = '\t'
        self.PER_PAGE_NUM_FILMS = 50

        if use_csv is None:
            self.files = self.getAllFiles(dirs["IMDB_FILMS_DIR"])
            self.coids = set()
            tot_ids = len(self.files)
            epochs = tot_ids // (self.THREADS * 4)
            if tot_ids % (self.THREADS * 4):
                epochs += 1
            start_epoch = 0
            for i in range(start_epoch, epochs):
                start_id = i * (self.THREADS * 4)
                end_id = (i + 1) * (self.THREADS * 4)
                self.items = []
                self.log.error("start %d:%d--%d" % (i, start_id, end_id))
                pool = ThreadPool(processes=self.THREADS)
                pool.map(self.get_coids, self.files[start_id:end_id])
                pool.close()
                pool.join()
            self.log.info("获取coids完成！")
            self.coids = list(self.coids)
            self.coids.sort()
            with open(dirs["ORI_ID_DIR"] + 'coids.csv', 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter='\t')
                writer.writerows(self.coids)
        else:
            self.coids = self.get_coids_file(use_csv['file'])

        tot_ids = len(self.coids)
        epochs = tot_ids // (self.THREADS * 4)
        if tot_ids % (self.THREADS * 4):
            epochs += 1
        start_epoch = 0
        for i in range(start_epoch, epochs):
            start_id = i * (self.THREADS * 4)
            end_id = (i + 1) * (self.THREADS * 4)
            self.items = []
            self.log.error("start %d:%d--%d" % (i, start_id, end_id))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.get_co_info, self.coids[start_id:end_id])
            pool.close()
            pool.join()
        self.log.info("爬取coids完成！")

    def get_co_info(self, url):
        item = Item()
        htmlContent = self.isok_getUrlInfo(url)
        if htmlContent is None:
            self.log.error("Can't get this url: %s" % url)
            return
        soup = BeautifulSoup(htmlContent, 'lxml')
        nums = soup.find("div", attrs={"class": "desc"}).find_all('span')
        nums = nums[0].get_text().strip()
        nums = re.findall('(\d+)', nums)
        if len(nums) >= 3:
            nums = ''.join(nums[2:])
        else:
            nums = ''.join(nums)
        tot_titles = int(nums)
        item.coid = re.search("companies=(tt\d+)", url).group(1).strip()
        header = soup.find('div', attrs={"class": "article"}).find('h1', attrs={"class": "header"}).get_text().strip()
        item.coName = re.search("With(.+)\(", header).group(1).strip()

        pages = tot_titles // self.PER_PAGE_NUM_FILMS
        if tot_titles % self.PER_PAGE_NUM_FILMS:
            pages += 1
        page_list = []
        for i in range(pages):
            cur_page = url + "&start=" + str(1 + i * self.PER_PAGE_NUM_FILMS)
            page_list.append(cur_page)

        pool = ThreadPool(processes=self.THREADS)
        res = pool.map_async(self.get_curPage_co, page_list)
        pool.close()
        pool.join()
        res.wait()
        if res.ready():
            if res.successful():
                tmp = res.get()
                # print(tmp)
                item.coFilms = []
                for i in range(pages):
                    item.coFilms.extend(tmp[i][0])

        self.pipeline_save_url(item)

    def pipeline_save_url(self, item):
        item_dir = self.save_pre_dir + item.id
        if not os.path.exists(item_dir):
            os.makedirs(item_dir)

    def get_curPage_co(self, page_url):
        htmlContent = self.isok_getUrlInfo(page_url)
        info_list = []
        soup = BeautifulSoup(htmlContent, 'lxml')
        list = soup.find("div", attrs={"class": "lister-list"})
        items = list.find_all('div', class_="lister-item")
        for li in items:
            cur_info = []
            li_img = li.find('div', class_="lister-item-image")
            img_url = li_img.img.attrs['src'].strip()
            li_content = li.find('div', attrs={"class": "lister-item-content"})
            title_id = li_content.h3.a.attrs["href"]
            title_id = re.search("/(tt\d+)", title_id).group(1).strip()
            title_name = li_content.h3.a.get_text().strip()
            cur_info.extend([title_id, title_name, img_url])
            certificate_node = li_content.find("span", attrs={"class": "certificate"})
            if certificate_node is not None:
                cur_info.append(certificate_node.get_text().strip())
            else:
                cur_info.append("")
            runtime_node = li_content.find("span", attrs={"class": "runtime"})
            if runtime_node is not None:
                cur_info.append(runtime_node.get_text().strip())
            genre_node = li_content.find("span", attrs={"class": "genre"})
            if genre_node is not None:
                cur_info.append(genre_node.get_text().strip())
            else:
                cur_info.append("")
            rating_node = li_content.find("div", class_="ratings-imdb-rating")
            if rating_node is not None:
                cur_info.append(rating_node.attrs['data-value'].strip())
            else:
                cur_info.append("")
            metascore_node = li_content.find("div", class_="ratings-metascore")
            if metascore_node is not None:
                cur_info.append(metascore_node.span.get_text().strip())
            else:
                cur_info.append("")
            describe_node = li_content.find("p", attrs={"class": "text-muted"})
            if describe_node is not None:
                cur_info.append(describe_node.get_text().strip())
            else:
                cur_info.append("")
            nms = li_content.find("p", attrs={"class": ""})
            if nms is not None:
                nm_names = nms.text.strip().replace('\n', '')
                a_links = nms.find_all('a')
                nm_names = nm_names.split("|")
                if len(nm_names) == 2:
                    directors_nms = nm_names[0].replace(' ', '').split(':')[1]
                    directors_nums = len(directors_nms.split(','))
                    directors_ids = []
                    for i in a_links[:directors_nums]:
                        cur_id = re.search("/(nm\d+)", i.attrs['href'].strip()).group(1).strip()
                        directors_ids.append(cur_id)
                    directors_ids = ','.join(directors_ids)
                    stars_nms = nm_names[1].replace(' ', '').split(":")[1]
                    starts_ids = []
                    for i in a_links[directors_nums:]:
                        cur_id = re.search("/(nm\d+)", i.attrs['href'].strip()).group(1).strip()
                        starts_ids.append(cur_id)
                    starts_ids = ','.join(starts_ids)
                    cur_info.extend([directors_nms, directors_ids, stars_nms, starts_ids])
                else:
                    cur_info.extend(["", ""])
                    stars_nms = nm_names[0].replace(' ', '').split(":")[1]
                    starts_ids = []
                    for i in a_links:
                        cur_id = re.search("/(nm\d+)", i.attrs['href'].strip()).group(1).strip()
                        starts_ids.append(cur_id)
                    starts_ids = ','.join(starts_ids)
                    cur_info.extend([stars_nms, starts_ids])
            else:
                cur_info.extend(["", "", "", ""])
            votes = li_content.find('p', attrs={"class": "sort-num_votes-visible"})
            if votes is not None:
                spans = votes.find_all('span')
                if len(spans) > 2:
                    votes_num = spans[1].attrs['data-value'].strip()
                    gross_num = spans[4].attrs['data-value'].strip()
                    gross_num = "$" + gross_num
                    cur_info.extend([votes_num, gross_num])
                else:
                    votes_num = spans[1].attrs['data-value'].strip()
                    cur_info.extend([votes_num, ""])
            else:
                cur_info.extend(["", ""])

            info_list.append(cur_info)
        return info_list

    def get_coids(self, cur_file):
        company_file = cur_file + '/companies.csv'
        data = pd.read_csv(company_file, sep=self.SEP)
        co_lists = data["ID"].values.tolist()
        sz = len(co_lists)
        for i in range(sz):
            co_lists[i] = "https://www.imdb.com/search/title/?companies=" + co_lists[i]
        self.coids.update(co_lists)

    def get_coids_file(self, file=None):
        file = dirs["ORI_ID_DIR"] + file
        data = pd.read_csv(file, sep='\t', header=None)
        titles = data.iloc[:, 0].values.tolist()
        titles = list(set(titles))
        titles.sort()
        self.sz = len(titles)
        for i in range(self.sz):
            titles[i] = "https://www.imdb.com/search/title/?companies=" + titles[i]
        return titles

    def getAllFiles(self, pre_dir):
        files = []
        count = 0
        for fn in os.listdir(pre_dir):
            if re.match("tt\d+", fn) and os.path.isdir(pre_dir + fn):
                count += 1
                files.append(pre_dir + fn)
        self.log.info("get titles:%d" % count)
        return files


class get_404_ttnm(baseSpider):
    def __init__(self):
        super().__init__()
        self.tts = self.get_all_titles('titles.csv', 'title')
        self.tt404 = []
        self.nms = self.get_all_titles('names.csv', 'name')
        self.nms404 = []
        self.log = MyLog('404.log')
        self.THREADS = 4

        self.tmp = []
        self.pattern = "/(tt\d+)"
        tot_files = len(self.tts)
        self.log.info('tot tts:%d' % tot_files)
        epochs = tot_files // (self.THREADS * 32)
        if tot_files % (self.THREADS * 32):
            epochs += 1
        self.log.info("tot epochs:%d" % epochs)
        for i in range(epochs):
            start_id = i * (self.THREADS * 32)
            end_id = (i + 1) * (self.THREADS * 32)
            self.log.error("start %d:%d--%d,tots:%d" % (i, start_id, end_id, tot_files))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.check, self.tts[start_id:end_id])
            pool.close()
            pool.join()
        self.tt404.extend(self.tmp)
        with open(dirs['ORI_ID_DIR'] + '404tts.csv', 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter='\t')
            self.tmp_tt404 = []
            for i in self.tt404:
                self.tmp_tt404.append([i])
            writer.writerows(self.tmp_tt404)

        self.tmp = []
        self.pattern = "/(nm\d+)"
        tot_files = len(self.nms)
        self.log.info('tot nms:%d' % tot_files)
        epochs = tot_files // (self.THREADS * 32)
        if tot_files % (self.THREADS * 32):
            epochs += 1
        self.log.info("tot epochs:%d" % epochs)
        for i in range(epochs):
            start_id = i * (self.THREADS * 32)
            end_id = (i + 1) * (self.THREADS * 32)
            self.log.error("start %d:%d--%d,tots:%d" % (i, start_id, end_id, tot_files))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.check, self.nms[start_id:end_id])
            pool.close()
            pool.join()
        self.nms404.extend(self.tmp)
        with open(dirs['ORI_ID_DIR'] + '404nms.csv', 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter='\t')
            self.tmp_nm404 = []
            for i in self.nms404:
                self.tmp_nm404.append([i])
            writer.writerows(self.tmp_nm404)

    def check(self, url):
        html = self.getResponseContent(url)
        if html == "404":
            self.tmp.append(re.search(self.pattern, url).group(1).strip())


class repair_video(baseSpider):
    def __init__(self, type, save_dir, epoch_file, log_file, is_reverse, THREADS):
        super().__init__()
        self.type = type
        self.is_reverse = is_reverse
        self.THREADS = THREADS
        self.save_pre_dir = save_dir
        self.base_download_dir = dirs['IMDB_DOWNLOAD_FILMS_DIR']
        if not os.path.exists(self.save_pre_dir):
            os.makedirs(self.save_pre_dir)
        self.log = MyLog(log_file)
        if type == 'tt':
            # self.urls = self.get_cur_titles(self.save_pre_dir, "tt\d+","title")
            self.urls = self.get_all_titles('titles.csv', 'title')
        else:
            # self.urls = self.get_cur_titles(self.save_pre_dir, "nm\d+", "name")
            self.urls = self.get_all_titles('names.csv', 'name')
        if self.is_reverse:
            self.urls.reverse()
        self.start_epoch_file = dirs['SPIDER_CUR_EPOCH_DIR'] + epoch_file
        if not os.path.exists(dirs['SPIDER_CUR_EPOCH_DIR']):
            os.makedirs(dirs['SPIDER_CUR_EPOCH_DIR'])
        self.PER_PAGE_OF_IMG = 48
        self.PER_PAGE_OF_VIDEO = 30
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('window-size=1920x1080')
        self.chrome_options.add_argument('–no-sandbox')
        self.chrome_options.add_argument('disable-cache')  # 禁用缓存
        self.chrome_options.add_argument('–disable-extensions')
        self.chrome_options.add_argument('--incognito')  # 无痕隐身模式
        self.chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])  # 设置为开发者模式
        self.chrome_options.add_argument('log-level=3')  # 禁用大量日志信息滚动输出
        # chrome_options.add_argument('user-agent=%s'%self.Header["User-Agent"])
        # self.chrome_options.add_argument('proxy-server=%s'%self.proxy["http"])
        self.chrome_options.add_argument('--ignore-certificate-errors')
        self.chrome_options.add_argument('--ignore-certificate-errors-spki-list')
        # self.seleniumwire_options = {
        #    'proxy': {
        #        'http': 'http://127.0.0.1:7890',
        #        'https': 'https://127.0.0.1:7890'
        #    }
        # }
        # self.seleniumwire_options = {
        #    'proxy': {
        #        'http': 'http://127.0.0.1:1080',
        #        'https': 'https://127.0.0.1:1080'
        #    }
        # }

        self.SEP = '\t'
        if type == "nm":
            self.base_dir = dirs['IMDB_DOWNLOAD_NMS_DIR']
        else:
            self.base_dir = dirs['IMDB_DOWNLOAD_FILMS_DIR']
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)
        self.base_video_dir = self.base_dir + 'videos/'
        if not os.path.exists(self.base_video_dir):
            os.makedirs(self.base_video_dir)
        self.video_remain_csv = self.base_video_dir + 'remain.csv'
        self.remain_pic_nums, self.remain_video_nums = 0, 0
        if not os.path.exists(self.video_remain_csv):
            with open(self.video_remain_csv, 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.SEP)
                writer.writerow(["ID", "download_url", "save_file"])

    def start_spider(self):
        tot_urls = len(self.urls)
        self.log.info("total ids:%d" % tot_urls)
        epochs = tot_urls // (self.THREADS * 2)
        if tot_urls % (self.THREADS * 2):
            epochs += 1
        start_epoch = 0
        if os.path.exists(self.start_epoch_file):
            with open(self.start_epoch_file, "r", encoding="utf8") as fi:
                lines = fi.readlines()
                start_epoch = int(lines[0]) + 1

        self.start_time = time.time()
        self.end_time = self.start_time
        for i in range(start_epoch, epochs):
            self.sum_dict = {
                "videos_samples": 0,
                "videos_download_nums": 0
            }
            start_id = i * (self.THREADS * 2)
            end_id = (i + 1) * (self.THREADS * 2)
            self.items = []
            self.log.error("start %d:%d--%d" % (i, start_id, end_id))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.spider, self.urls[start_id:end_id])
            pool.close()
            pool.join()
            with open(self.start_epoch_file, "w", encoding="utf8") as fi:
                fi.write(str(i))
            if i % 2 == 0:
                shutil.copy(self.start_epoch_file, self.start_epoch_file + '.bak.txt')

            if self.type == 'tt':
                json_file = "./sum_tt_download_videos_info.json"
                json_bak_file = "./sum_tt_download_videos_info_bak.json"
            else:
                json_file = './sum_nm_download_videos_info.json'
                json_bak_file = "./sum_nm_download_videos_info_bak.json"
            if i % 2 == 0:
                shutil.copy(json_file, json_bak_file)
            with open(json_file, 'r') as load_f:
                load_dict = json.load(load_f)
            for cur_key in load_dict.keys():
                load_dict[cur_key] += self.sum_dict[cur_key]
                self.sum_dict[cur_key] = 0
            json_str = json.dumps(load_dict, indent=4)
            self.log.info("sum_info: " + json_str)
            with open(json_file, "w") as f:
                f.write(json_str)

        self.end_time = time.time()
        total_time = self.end_time - self.start_time
        self.log.info('爬取数据信息及链接完成，总用时： {:.0f}m {:.0f}s'.format(total_time // 60, total_time % 60))

    def spider(self, url):
        item = Item()
        video_url = url + "/videogallery"  # 电影相关视频

        if self.type == 'tt':
            item.id = re.search("/(tt\d+)", url).group(1).strip()

            item_dir = self.save_pre_dir + item.id
            # videos_csv = item_dir + '/videos.csv'
            # if os.path.exists(videos_csv):
            #     self.log.info("%s 已经爬取" % url)
            #     return

            htmlContent = self.isok_getUrlInfo(url)
            if htmlContent is None:
                self.log.error("Can't get this url: %s" % url)
                if os.path.exists(self.save_pre_dir + item.id):
                    delete_dir(self.save_pre_dir + item.id)
                return

            soup = BeautifulSoup(htmlContent, 'lxml')
            try:
                item.movieName = soup.find('div', attrs={"class": "title_wrapper"}).h1.get_text().strip()
                rating = soup.find('span', attrs={"class": "rating"})
                if rating != None:
                    item.IMDB_Rating = rating.get_text().strip()
                else:
                    item.IMDB_Rating = ""
                metascore = soup.find('div', attrs={"class": "plot_summary_wrapper"}).find('div',
                                                                                           class_="metacriticScore")
                if metascore != None:
                    item.Metascore = metascore.get_text().strip()
                else:
                    item.Metascore = ""
            except:
                item.movieName = soup.find('h1', attrs={"data-testid": "hero-title-block__title"}).get_text().strip()
                rating = soup.find('div', attrs={"data-testid": "hero-title-block__aggregate-rating__score"})
                if rating != None:
                    item.IMDB_Rating = rating.get_text().strip()
                else:
                    item.IMDB_Rating = ""
                metascore = soup.find('span', attrs={"class": "score-meta"})
                if metascore != None:
                    item.Metascore = metascore.get_text().strip()
                else:
                    item.Metascore = ""
            soup.decompose()
        else:
            item.id = re.search("/(nm\d+)", url).group(1).strip()

            item_dir = self.save_pre_dir + item.id
            videos_csv = item_dir + '/videos.csv'
            if os.path.exists(videos_csv):
                self.log.info("%s 已经爬取" % url)
                return

            htmlContent = self.isok_getUrlInfo(url)
            if htmlContent is None:
                self.log.error("Can't get this url:%s" % url)
                if os.path.exists(self.save_pre_dir + item.id):
                    delete_dir(self.save_pre_dir + item.id)
                return
            soup = BeautifulSoup(htmlContent, 'lxml')

            header = soup.find('h1', attrs={"class": "header"})
            nmName = header.find('span', attrs={"class": "itemprop"})
            while nmName == None:
                htmlContent = self.isok_getUrlInfo(url)
                soup = BeautifulSoup(htmlContent, 'lxml')
                header = soup.find('h1', attrs={"class": "header"})
                nmName = header.find('span', attrs={"class": "itemprop"})
            item.nmName = nmName.get_text().strip()
            jobs = soup.find('div', attrs={"id": "name-job-categories", "class": "infobar"})
            if jobs != None:
                jobs = jobs.find_all('a')
                item.jobs = '|'.join([i.get_text().strip() for i in jobs])
            else:
                item.jobs = ''
            born = soup.find('div', attrs={"id": "name-born-info"})
            if born != None:
                born = born.strings
                item.born = ''.join([i.strip() for i in born])
            else:
                item.born = ''
            soup.decompose()

        # 3、电影相关视频
        html3 = self.isok_getUrlInfo(video_url)
        if html3 is None:
            self.log.error("Can't get this url: %s" % video_url)
            return
        soup3 = BeautifulSoup(html3, 'lxml')
        desc = soup3.find('span', attrs={"id": "vg-left"})
        if desc != None:
            video_nums = desc.get_text().strip()
            video_nums = re.findall('(\d+)', video_nums)
            if len(video_nums) >= 3:
                video_nums = ''.join(video_nums[2:])
            else:
                video_nums = ''.join(video_nums)
            video_nums = int(video_nums)
            self.getAllvideos(item, video_url, video_nums)
        soup3.decompose()

        self.log.info("获取%s结束" % item.movieName)
        self.pipeline_save_url(item)

    def pipeline_save_url(self, item):
        item_dir = self.save_pre_dir + item.id
        if not os.path.exists(item_dir):
            os.makedirs(item_dir)
        self.save_video_info(item, item_dir)
        self.log.info("%s写入文件成功" % (item.id))
        self.log.info("start downloading...")
        self.download_videos(item)

    def save_video_info(self, item, item_dir):
        videos_csv = item_dir + '/videos.csv'
        with open(videos_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["url", "ID"])
            if item.videos != None:
                self.log.info("save : %d videos %d" % (len(item.videos['ID']), len(item.videos['url'])))
                all_video = list(zip(item.videos["url"], item.videos["ID"]))
                writer.writerows(all_video)

    def getAllvideos(self, item, url, nums):
        self.log.info("%s有%s个视频" % (item.id, nums))
        pages = nums // self.PER_PAGE_OF_VIDEO
        if nums % self.PER_PAGE_OF_VIDEO:
            pages += 1
        page_url = []
        for i in range(1, pages + 1):
            page_url.append(url + '?page=' + str(i))
        item.videos = {'url': [], "ID": []}
        tmp_videos = []
        for cur_url in page_url:
            htmlContent = self.isok_getUrlInfo(cur_url)
            soup = BeautifulSoup(htmlContent, 'lxml')
            videos = soup.find('div', attrs={"class": re.compile("search-results")})
            videos = videos.find_all('div', attrs={"class": re.compile("results-item")})
            tmp_videos.extend(videos)
            soup.decompose()
        pool = ThreadPool(processes=self.THREADS)
        res = pool.map_async(self.get_cur_video, tmp_videos)
        pool.close()
        pool.join()
        res.wait()
        if res.ready():
            if res.successful():
                tmp = res.get()
                for i in range(len(tmp)):
                    if tmp[i] is None:
                        continue
                    item.videos['url'].append(tmp[i][0])
                    item.videos['ID'].append(tmp[i][1])
        self.log.info("%s tot videos:%d" % (item.id, len(item.videos['url'])))

    def get_cur_video(self, video):
        video_url = video.a.attrs['data-video']
        id = video_url
        video_url = "https://www.imdb.com/videoplayer/" + video_url
        mp4Url = ''
        cnt_time = 0
        while mp4Url == '':
            video_html = self.isok_getUrlInfo(video_url)
            if video_html is None:
                self.log.error("Can't get this url:%s" % video_url)
                return '', id
            if cnt_time > 50:
                self.log.error("Can't get this video:%s" % video_url)
                return '', id
            video_soup = BeautifulSoup(video_html, "lxml")
            scripts = video_soup.find_all('script', attrs={"type": "text/javascript"})
            for script in scripts:
                text = script.get_text()
                text2 = script.string
                if text2 != None and text2 != '':
                    text = text + '\n' + text2
                urls = re.findall('"videoUrl":"(\S+?)"', text)
                urls2 = re.findall('\\\\"url\\\\":\\\\"(\S+?)\\\\"', text)
                urls.extend(urls2)
                for url in urls:
                    type = re.search('\.mp4\?', url)
                    # type2=re.search('pgv4ql', url)
                    if type:
                        mp4Url = url
                        break
                if mp4Url == '':
                    continue
                else:
                    break
            video_soup.decompose()
            cnt_time += 1
        # realUrl = re.search('(.+)u002Fvi(.+)\\\\u002F(.+?)\Z', mp4Url).groups()
        # realUrl = 'https://imdb-video.media-imdb.com/vi' + realUrl[1] + '/' + realUrl[2]
        self.log.info("video url:%s" % mp4Url)
        return mp4Url, id

    def download_videos(self, item):
        if item.videos != None:
            try:
                videos = item.videos['url']
                ids = item.videos['ID']
                for pos in range(len(ids)):
                    cur_id = ids[pos]
                    video_id_dir = self.base_video_dir + cur_id + '/'
                    if not os.path.exists(video_id_dir):
                        os.makedirs(video_id_dir)
                        self.sum_dict['videos_samples'] += 1
                    video_file = video_id_dir + cur_id + ".mp4"
                    if not os.path.exists(video_file):
                        self.download_for_video(cur_id, videos[pos], video_file)
            except Exception as e:
                self.log.info("e")
        else:
            self.log.info('无有效video url')

    def download_for_video(self, id, url, file):
        if url == '':
            self.log.error("获取视频%s 失败, 无效url" % id)
            return
        cnt = 0
        while cnt <= 20:
            try:
                r = requests.get(url, timeout=(20, 20), stream=True)
                with open(file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                self.log.info("获取视频%s 成功" % url)
                self.sum_dict['videos_download_nums'] += 1
                return
            except:
                cnt += 1
        self.log.error("获取视频%s 失败" % url)
        with open(self.video_remain_csv, 'a+', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.SEP)
            writer.writerow([id, url, file])
        self.remain_video_nums += 1


class repair_img(baseSpider):
    def __init__(self, type, save_dir, epoch_file, log_file, is_reverse, THREADS):
        super().__init__()
        self.type = type
        self.is_reverse = is_reverse
        self.THREADS = THREADS
        self.save_pre_dir = save_dir
        if not os.path.exists(self.save_pre_dir):
            os.makedirs(self.save_pre_dir)
        self.log = MyLog(log_file)
        if type == 'tt':
            self.urls = self.get_all_titles("titles.csv", "title")
        else:
            self.urls = self.get_all_titles('names.csv', "name")
        if self.is_reverse:
            self.urls.reverse()
        self.start_epoch_file = dirs['SPIDER_CUR_EPOCH_DIR'] + epoch_file
        if not os.path.exists(dirs['SPIDER_CUR_EPOCH_DIR']):
            os.makedirs(dirs['SPIDER_CUR_EPOCH_DIR'])
        self.PER_PAGE_OF_IMG = 48
        self.PER_PAGE_OF_VIDEO = 30
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('window-size=1920x1080')
        self.chrome_options.add_argument('–no-sandbox')
        self.chrome_options.add_argument('disable-cache')  # 禁用缓存
        self.chrome_options.add_argument('–disable-extensions')
        self.chrome_options.add_argument('--incognito')  # 无痕隐身模式
        self.chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])  # 设置为开发者模式
        self.chrome_options.add_argument('log-level=3')  # 禁用大量日志信息滚动输出
        # chrome_options.add_argument('user-agent=%s'%self.Header["User-Agent"])
        # self.chrome_options.add_argument('proxy-server=%s'%self.proxy["http"])
        self.chrome_options.add_argument('--ignore-certificate-errors')
        self.chrome_options.add_argument('--ignore-certificate-errors-spki-list')
        # self.seleniumwire_options = {
        #    'proxy': {
        #        'http': 'http://127.0.0.1:7890',
        #        'https': 'https://127.0.0.1:7890'
        #    }
        # }
        # self.seleniumwire_options = {
        #    'proxy': {
        #        'http': 'http://127.0.0.1:1080',
        #        'https': 'https://127.0.0.1:1080'
        #    }
        # }

        self.SEP = '\t'
        if type == "nm":
            self.base_dir = dirs['IMDB_DOWNLOAD_NMS_DIR']
        else:
            self.base_dir = dirs['IMDB_DOWNLOAD_FILMS_DIR']
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)
        self.base_img_dir = self.base_dir + 'pics/'
        if not os.path.exists(self.base_img_dir):
            os.makedirs(self.base_img_dir)
        self.remain_pic_nums = 0
        self.pic_remain_csv = self.base_img_dir + 'remain.csv'
        if not os.path.exists(self.pic_remain_csv):
            with open(self.pic_remain_csv, 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.SEP)
                writer.writerow(["ID", "download_url", "save_file"])

    def start_spider(self):
        tot_urls = len(self.urls)
        self.log.info("total ids:%d" % tot_urls)
        epochs = tot_urls // (self.THREADS * 2)
        if tot_urls % (self.THREADS * 2):
            epochs += 1
        start_epoch = 0
        if os.path.exists(self.start_epoch_file):
            with open(self.start_epoch_file, "r", encoding="utf8") as fi:
                lines = fi.readlines()
                start_epoch = int(lines[0]) + 1

        self.start_time = time.time()
        self.end_time = self.start_time
        for i in range(start_epoch, epochs):
            self.sum_dict = {
                "pics_samples": 0,
                "pics_download_nums": 0,
            }
            start_id = i * (self.THREADS * 2)
            end_id = (i + 1) * (self.THREADS * 2)
            self.items = []
            self.log.error("start %d:%d--%d" % (i, start_id, end_id))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.spider, self.urls[start_id:end_id])
            pool.close()
            pool.join()
            with open(self.start_epoch_file, "w", encoding="utf8") as fi:
                fi.write(str(i))
            if i % 2 == 0:
                shutil.copy(self.start_epoch_file, self.start_epoch_file + '.bak.txt')

            if self.type == 'tt':
                json_file = "./sum_tt_download_pics_info.json"
                json_bak_file = "./sum_tt_download_pics_info_bak.json"
            else:
                json_file = './sum_nm_download_pics_info.json'
                json_bak_file = "./sum_nm_download_pics_info_bak.json"
            if i % 2 == 0:
                shutil.copy(json_file, json_bak_file)
            with open(json_file, 'r') as load_f:
                load_dict = json.load(load_f)
            for cur_key in load_dict.keys():
                load_dict[cur_key] += self.sum_dict[cur_key]
                self.sum_dict[cur_key] = 0
            json_str = json.dumps(load_dict, indent=4)
            self.log.info("sum_info: " + json_str)
            with open(json_file, "w") as f:
                f.write(json_str)

        self.end_time = time.time()
        total_time = self.end_time - self.start_time
        self.log.info('爬取数据信息及链接完成，总用时： {:.0f}m {:.0f}s'.format(total_time // 60, total_time % 60))

    def spider(self, url):
        item = Item()
        img_url = url + "/mediaindex"  # 电影相关图片

        if self.type == 'tt':
            item.id = re.search("/(tt\d+)", url).group(1).strip()

            item_dir = self.save_pre_dir + item.id
            # imgs_csv = item_dir + '/imgs.csv'
            # if os.path.exists(imgs_csv):
            #     self.log.info("%s 已经爬取" % url)
            #     return

            htmlContent = self.isok_getUrlInfo(url)
            if htmlContent is None:
                self.log.error("Can't get this url: %s" % url)
                if os.path.exists(self.save_pre_dir + item.id):
                    delete_dir(self.save_pre_dir + item.id)
                return

            soup = BeautifulSoup(htmlContent, 'lxml')
            try:
                item.movieName = soup.find('div', attrs={"class": "title_wrapper"}).h1.get_text().strip()
                rating = soup.find('span', attrs={"class": "rating"})
                if rating != None:
                    item.IMDB_Rating = rating.get_text().strip()
                else:
                    item.IMDB_Rating = ""
                metascore = soup.find('div', attrs={"class": "plot_summary_wrapper"}).find('div',
                                                                                           class_="metacriticScore")
                if metascore != None:
                    item.Metascore = metascore.get_text().strip()
                else:
                    item.Metascore = ""
            except:
                item.movieName = soup.find('h1', attrs={"data-testid": "hero-title-block__title"}).get_text().strip()
                rating = soup.find('div', attrs={"data-testid": "hero-title-block__aggregate-rating__score"})
                if rating != None:
                    item.IMDB_Rating = rating.get_text().strip()
                else:
                    item.IMDB_Rating = ""
                metascore = soup.find('span', attrs={"class": "score-meta"})
                if metascore != None:
                    item.Metascore = metascore.get_text().strip()
                else:
                    item.Metascore = ""
            soup.decompose()
        else:
            item.id = re.search("/(nm\d+)", url).group(1).strip()

            item_dir = self.save_pre_dir + item.id
            imgs_csv = item_dir + '/imgs.csv'
            if os.path.exists(imgs_csv):
                self.log.info("%s 已经爬取" % url)
                return

            htmlContent = self.isok_getUrlInfo(url)
            if htmlContent is None:
                self.log.error("Can't get this url:%s" % url)
                if os.path.exists(self.save_pre_dir + item.id):
                    delete_dir(self.save_pre_dir + item.id)
                return
            soup = BeautifulSoup(htmlContent, 'lxml')

            header = soup.find('h1', attrs={"class": "header"})
            nmName = header.find('span', attrs={"class": "itemprop"})
            while nmName == None:
                htmlContent = self.isok_getUrlInfo(url)
                soup = BeautifulSoup(htmlContent, 'lxml')
                header = soup.find('h1', attrs={"class": "header"})
                nmName = header.find('span', attrs={"class": "itemprop"})
            item.nmName = nmName.get_text().strip()
            jobs = soup.find('div', attrs={"id": "name-job-categories", "class": "infobar"})
            if jobs != None:
                jobs = jobs.find_all('a')
                item.jobs = '|'.join([i.get_text().strip() for i in jobs])
            else:
                item.jobs = ''
            born = soup.find('div', attrs={"id": "name-born-info"})
            if born != None:
                born = born.strings
                item.born = ''.join([i.strip() for i in born])
            else:
                item.born = ''
            soup.decompose()

        # 2、电影相关图片
        html2 = self.isok_getUrlInfo(img_url)
        if html2 is None:
            self.log.error("Can't get this url: %s" % img_url)
            return
        soup2 = BeautifulSoup(html2, 'lxml')
        leftright = soup2.find('div', attrs={"id": "media_index_content", "class": "header"})
        if leftright != None:
            photos = leftright.find('div', attrs={"id": "left", "class": "desc"})
            if photos != None:
                photos = photos.get_text().strip()
                nums = re.findall('(\d+)', photos)
                if len(nums) >= 3:
                    nums = ''.join(nums[2:])
                else:
                    nums = ''.join(nums)
                nums = int(nums)
                self.getAllimgs(item, img_url, nums)
        soup2.decompose()

        self.log.info("获取%s结束" % item.movieName)
        self.pipeline_save_url(item)

    def pipeline_save_url(self, item):
        item_dir = self.save_pre_dir + item.id
        if not os.path.exists(item_dir):
            os.makedirs(item_dir)
        self.save_img_info(item, item_dir)
        self.log.info("%s写入文件成功" % (item.id))
        self.log.info("start downloading...")

        self.download_pics(item)

    def save_img_info(self, item, item_dir):
        imgs_csv = item_dir + '/imgs.csv'
        with open(imgs_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["small", "large", "ID", "ori"])
            if item.imgs != None:
                self.log.info("save: %d small pics %d,large pics %d,ori pics %d" % (
                    len(item.imgs["ID"]), len(item.imgs["small"]), len(item.imgs["large"]), len(item.imgs["ori"])))
                all_img = list(zip(item.imgs["small"], item.imgs["large"], item.imgs["ID"], item.imgs["ori"]))
                writer.writerows(all_img)

    def getAllimgs(self, item, url, nums):
        self.log.info("%s有%s张图片" % (item.id, nums))
        pages = nums // self.PER_PAGE_OF_IMG
        if nums % self.PER_PAGE_OF_IMG:
            pages += 1
        page_url = []
        # orlList=[]
        for i in range(1, pages + 1):
            page_url.append(url + '?page=' + str(i))
        pool = ThreadPool(processes=self.THREADS)
        res = pool.map_async(self.get_curUrl_img, page_url)
        pool.close()
        pool.join()
        res.wait()
        if res.ready():
            if res.successful():
                tmp = res.get()
                # print(tmp)
                item.imgs = {"small": [], "large": [], "ID": [], "ori": []}
                for i in range(pages):
                    item.imgs["small"].extend(tmp[i][0])
                    item.imgs["large"].extend(tmp[i][1])
                    item.imgs["ID"].extend(tmp[i][2])
                    item.imgs["ori"].extend(tmp[i][3])

    def get_curUrl_img(self, cur_url):
        htmlContent = self.isok_getUrlInfo(cur_url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        imgs = soup.find('div', attrs={"class": "media_index_thumb_list", "id": "media_index_thumbnail_grid"})
        imgs = imgs.find_all('img')
        small_list = []
        large_list = []
        ori_list = []
        id_list = []
        for img in imgs:
            img_url = img.attrs['src']
            small_list.append(img_url)
        orlList = []
        for img in imgs:
            img_ori_url = "https://www.imdb.com" + img.parent.attrs['href']
            orlList.append(img_ori_url)
            id = re.search("/(rm\d+)", img_ori_url).group(1).strip()
            id_list.append(id)
        pool = ThreadPool(processes=self.THREADS)
        res = pool.map_async(self.get_ori_imgs, orlList)
        pool.close()
        pool.join()
        res.wait()
        if res.ready():
            if res.successful():
                large_list = res.get()
        while len(small_list) != len(large_list):
            self.log.error("获取大图失败，重新尝试中")
            pool = ThreadPool(processes=self.THREADS)
            res = pool.map_async(self.get_ori_imgs, orlList)
            pool.close()
            pool.join()
            res.wait()
            if res.ready():
                if res.successful():
                    large_list = res.get()
        soup.decompose()
        for i in large_list:
            tmp = i.split('.')
            ori_img = tmp[:3] + tmp[-1:]
            ori_img = '.'.join(ori_img)
            ori_list.append(ori_img)
        return small_list, large_list, id_list, ori_list, orlList

    def get_ori_imgs(self, img_url):
        htmlContent2 = self.isok_getUrlInfo(img_url)
        if htmlContent2 is None:
            self.log.error("%s 获取地址出错" % img_url)
            return ''
        soup2 = BeautifulSoup(htmlContent2, 'lxml')
        img_ori_url = soup2.find('meta', attrs={"property": "og:image"}).attrs['content']
        if img_ori_url is None:
            self.log.error("%s 获取地址出错" % img_url)
        soup2.decompose()
        return img_ori_url

    def download_pics(self, item):
        if item.imgs != None:
            try:
                smalls = item.imgs['small']
                larges = item.imgs['large']
                oris = []
                for i in larges:
                    tmp = i.split('.')
                    ori_img = tmp[:3] + tmp[-1:]
                    ori_img = '.'.join(ori_img)
                    oris.append(ori_img)
                ids = item.imgs['ID']
                for pos in range(len(ids)):
                    cur_id = ids[pos]
                    pic_id_dir = self.base_img_dir + cur_id + '/'
                    if not os.path.exists(pic_id_dir):
                        os.makedirs(pic_id_dir)
                        self.sum_dict["pics_samples"] += 1
                    small_file = pic_id_dir + cur_id + "_small.jpg"
                    large_file = pic_id_dir + cur_id + "_large.jpg"
                    ori_file = pic_id_dir + cur_id + "_ori.jpg"
                    if not os.path.exists(small_file):
                        self.download_for_ipg(cur_id, smalls[pos], small_file)
                    if not os.path.exists(large_file):
                        self.download_for_ipg(cur_id, larges[pos], large_file)
                    if not os.path.exists(ori_file):
                        self.download_for_ipg(cur_id, oris[pos], ori_file)
            except Exception as e:
                self.log.info(e)
        else:
            self.log.info('无有效img url')

    def download_for_ipg(self, id, url, file):
        if url == '':
            return
        cnt = 0
        while cnt <= 20:
            try:
                r = requests.get(url, timeout=(10, 10), stream=True)
                with open(file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        f.write(chunk)
                self.log.info("获取图片%s 成功" % url)
                self.sum_dict["pics_download_nums"] += 1
                return
            except:
                cnt += 1
        self.log.error("获取图片%s 失败" % url)
        with open(self.pic_remain_csv, 'a+', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.SEP)
            writer.writerow([id, url, file])
        self.remain_pic_nums += 1


class supplement_reviews(films_spider):
    def __init__(self, use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS):
        super().__init__(use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS)

    def start_spider(self):
        tot_urls = len(self.urls)
        self.log.info("total ids:%d" % tot_urls)
        epochs = tot_urls // (self.THREADS * 2)
        if tot_urls % (self.THREADS * 2):
            epochs += 1
        start_epoch = 0
        if os.path.exists(self.start_epoch_file):
            with open(self.start_epoch_file, "r", encoding="utf8") as fi:
                lines = fi.readlines()
                start_epoch = int(lines[0]) + 1

        self.start_time = time.time()
        self.end_time = self.start_time
        for i in range(start_epoch, epochs):
            self.sum_dict = {
                "reviews_samples": 0,
                "reviews_attributes": 0,
                "data_Reviews_crawled": 0,
            }
            start_id = i * (self.THREADS * 2)
            end_id = (i + 1) * (self.THREADS * 2)
            self.items = []
            self.log.error("start %d:%d--%d" % (i, start_id, end_id))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.spider, self.urls[start_id:end_id])
            pool.close()
            pool.join()
            with open(self.start_epoch_file, "w", encoding="utf8") as fi:
                fi.write(str(i))
            if i % 2 == 0:
                shutil.copy(self.start_epoch_file, self.start_epoch_file + '.bak.txt')

            json_file = "./data_Reviews_sum_tt_info.json"
            json_bak_file = "./data_Reviews_sum_tt_info_bak.json"
            if i % 2 == 0:
                shutil.copy(json_file, json_bak_file)
            with open(json_file, 'r') as load_f:
                load_dict = json.load(load_f)
            for cur_key in load_dict.keys():
                load_dict[cur_key] += self.sum_dict[cur_key]
                self.sum_dict[cur_key] = 0
            json_str = json.dumps(load_dict, indent=4)
            self.log.info("sum_info: " + json_str)
            with open(json_file, "w") as f:
                f.write(json_str)

        self.end_time = time.time()
        total_time = self.end_time - self.start_time
        self.log.info('爬取数据信息及链接完成，总用时： {:.0f}m {:.0f}s'.format(total_time // 60, total_time % 60))

    def spider(self, url):
        item = Item()
        item.id = re.search("/(tt\d+)", url).group(1).strip()
        reviews_csv = self.save_pre_dir + item.id + '/reviews.csv'
        if not self.is_repair:
            if os.path.exists(reviews_csv):
                self.log.info("%s 已经爬取" % url)
                return
        review_url = url + "/reviews?sort=submissionDate&dir=asc"  # 评论
        self.getReviews(item, review_url)
        self.log.info('%s:review is over' % item.id)
        self.log.info("获取%s成功" % item.id)
        self.pipeline_save_url(item)

    def pipeline_save_url(self, item):
        item_dir = self.save_pre_dir + item.id
        if not os.path.exists(item_dir):
            os.makedirs(item_dir)
        # 1、评论[["rating","title","name","date","text"]]
        self.save_reviews_csv(item, item_dir)
        self.log.info("%s写入文件成功" % (item.id))

    def save_reviews_csv(self, item, item_dir):
        reviews_csv = item_dir + '/reviews.csv'
        ori_len = 0
        ori_crawled = 0
        if os.path.exists(reviews_csv):
            ori_crawled = 1
            with open(reviews_csv, 'r', encoding="utf8") as fi:
                lines = fi.readlines()
                ori_len = (len(lines) - 1)
        with open(reviews_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["rating", "title", "name", "ID", "date", "text", "action"])
            # ["rating", "title", "name", "ID", "date", "text", "action"]
            if item.reviews != None:
                writer.writerows(item.reviews)
                self.sum_dict["reviews_samples"] += len(item.reviews) - ori_len
                self.sum_dict["reviews_attributes"] += (len(item.reviews) - ori_len) * 7
                self.sum_dict["data_Reviews_crawled"] += 1 - ori_crawled

    def getReviews(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        if htmlContent is None:
            self.log.error("Can't get this url: %s" % url)
            if os.path.exists(self.save_pre_dir + item.id):
                delete_dir(self.save_pre_dir + item.id)
            return
        soup = BeautifulSoup(htmlContent, 'lxml')
        button = soup.find('button', attrs={"id": "load-more-trigger"})

        if button != None:
            self.revirews_selenium(item, url)
            return

        reviews = soup.find_all('div', class_="lister-item")
        if reviews != []:
            self.log.info("%s tot reviews:%d" % (item.id, len(reviews)))
            pool = ThreadPool(processes=self.THREADS)
            res = pool.map_async(self.getEachReview, reviews)
            pool.close()
            pool.join()
            res.wait()
            if res.ready():
                if res.successful():
                    item.reviews = res.get()
        soup.decompose()

    def revirews_selenium(self, item, url):
        self.log.info("selenium:%s" % url)
        driver = webdriver.Chrome(chrome_options=self.chrome_options)
        # tot_reviews=None
        while True:
            try:
                driver.get(url)
                htmlcontent = driver.page_source
                soup_tmp = BeautifulSoup(htmlcontent, 'lxml')
                nums = soup_tmp.find('div', attrs={"class": "lister"}).find("div", attrs={"class": "header"}).find(
                    'div', recursive=False).find('span').get_text().strip()
                nums = re.findall('(\d+)', nums)
                if len(nums) >= 3:
                    nums = ''.join(nums[2:])
                else:
                    nums = ''.join(nums)
                tot_reviews = int(nums)
                soup_tmp.decompose()
                break
            except Exception as e:
                self.log.error("selenium: 获取driver失败，重试中(%s)" % e)
        self.log.info("%s has %d reviews" % (url, tot_reviews))

        max_try = 0
        get_tot_flag = False
        while max_try < 10 and not get_tot_flag:
            repeat_cnt = 0
            pre_len = -1
            max_try += 1
            while True:
                t = True
                while t:
                    check_height = driver.execute_script("return document.body.scrollHeight;")
                    js = "var q=document.documentElement.scrollTop=" + str(check_height + 500)
                    driver.execute_script(js)
                    driver.implicitly_wait(3)
                    time.sleep(3)
                    check_height1 = driver.execute_script("return document.body.scrollHeight;")
                    self.log.info(str(item.id) + ': ' + str(check_height) + '**************' + str(check_height1))
                    if check_height == check_height1:
                        t = False
                try:
                    next_cn =driver.find_element(By.ID,'load-more-trigger') #driver.find_element_by_id('load-more-trigger')
                    driver.execute_script("arguments[0].scrollIntoView();", next_cn)
                    next_cn.click()
                    driver.implicitly_wait(5)
                    time.sleep(5)
                except Exception as e:
                    self.log.info("%s  selenium over!(%s)" % (url, e))
                htmlcontent = driver.page_source
                soup = BeautifulSoup(htmlcontent, 'lxml')
                reviews = soup.find('div', attrs={"class": "lister-list"}).find_all('div', class_="lister-item")
                self.log.info("%s get reviews:%d/%d" % (item.id, len(reviews), tot_reviews))
                if pre_len == -1:
                    pre_len = len(reviews)
                    repeat_cnt = 0
                else:
                    if len(reviews) == pre_len:
                        repeat_cnt += 1
                if repeat_cnt >= 10:
                    soup.decompose()
                    break
                load_all = soup.find("div", attrs={"class": "lister"}).find('div',
                                                                            attrs={"class": re.compile("loaded-all")})
                if load_all is not None or reviews == tot_reviews:
                    self.log.info("%s  selenium over!" % url)
                    soup.decompose()
                    get_tot_flag = True
                    break
                soup.decompose()

        if reviews != []:
            pool = ThreadPool(processes=self.THREADS)
            res = pool.map_async(self.getEachReview, reviews)
            pool.close()
            pool.join()
            res.wait()
            if res.ready():
                if res.successful():
                    item.reviews = res.get()
        driver.delete_all_cookies()
        driver.close()
        driver.quit()

    def getEachReview(self, review):
        cur_review = []  # {"rating":"","title":"","name":"","ID":"","date":"","text":"","action":""}
        rating = review.find('span', attrs={"class": "rating-other-user-rating"})
        if rating != None:
            cur_review.append(rating.get_text().strip())
        else:
            cur_review.append("")
        title = review.find('a', attrs={"class": "title"})
        if title != None:
            cur_review.append(title.get_text().strip())
        else:
            cur_review.append("")
        name_date = review.find('div', attrs={"class": "display-name-date"})
        if name_date != None:
            cur_review.append(name_date.a.get_text().strip())
            cur_review.append(re.search("/(ur\d+)", name_date.a.attrs['href'].strip()).group(1).strip())
            cur_review.append(name_date.find('span', attrs={"class": "review-date"}).get_text().strip())
        else:
            cur_review.append("")
            cur_review.append("")
            cur_review.append("")
        content = review.find('div', attrs={"class": "content"}).find('div', class_="text")
        if content != None:
            content_strings = list(content.strings)
            tot_txt = ""
            for cur_string in content_strings:
                if tot_txt != "" and cur_string.strip() != "":
                    tot_txt += ' '
                tot_txt += cur_string.strip().replace('\n', ' ')
            cur_review.append(tot_txt)
        else:
            cur_review.append("")
        action = review.find('div', class_="actions text-muted")
        if action != None:
            cur_review.append(action.contents[0].strip())
        else:
            cur_review.append("")
        return cur_review


class supplement_news(films_spider):
    def __init__(self, use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS):
        super().__init__(use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS)

    def start_spider(self):
        tot_urls = len(self.urls)
        self.log.info("total ids:%d" % tot_urls)
        epochs = tot_urls // (self.THREADS * 2)
        if tot_urls % (self.THREADS * 2):
            epochs += 1
        start_epoch = 0
        if os.path.exists(self.start_epoch_file):
            with open(self.start_epoch_file, "r", encoding="utf8") as fi:
                lines = fi.readlines()
                start_epoch = int(lines[0]) + 1

        self.start_time = time.time()
        self.end_time = self.start_time
        for i in range(start_epoch, epochs):
            self.sum_dict = {
                "news_samples": 0,
                "news_attributes": 0,
                "data_news_crawled": 0,
            }
            start_id = i * (self.THREADS * 2)
            end_id = (i + 1) * (self.THREADS * 2)
            self.items = []
            self.log.error("start %d:%d--%d" % (i, start_id, end_id))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.spider, self.urls[start_id:end_id])
            pool.close()
            pool.join()
            with open(self.start_epoch_file, "w", encoding="utf8") as fi:
                fi.write(str(i))
            if i % 2 == 0:
                shutil.copy(self.start_epoch_file, self.start_epoch_file + '.bak.txt')

            json_file = "./data_news_sum_tt_info.json"
            json_bak_file = "./data_news_sum_tt_info_bak.json"
            if i % 2 == 0:
                shutil.copy(json_file, json_bak_file)
            with open(json_file, 'r') as load_f:
                load_dict = json.load(load_f)
            for cur_key in load_dict.keys():
                load_dict[cur_key] += self.sum_dict[cur_key]
                self.sum_dict[cur_key] = 0
            json_str = json.dumps(load_dict, indent=4)
            self.log.info("sum_info: " + json_str)
            with open(json_file, "w") as f:
                f.write(json_str)

        self.end_time = time.time()
        total_time = self.end_time - self.start_time
        self.log.info('爬取数据信息及链接完成，总用时： {:.0f}m {:.0f}s'.format(total_time // 60, total_time % 60))

    def spider(self, url):
        item = Item()
        item.id = re.search("/(tt\d+)", url).group(1).strip()
        news_csv = self.save_pre_dir + item.id + '/news.csv'
        if not self.is_repair:
            if os.path.exists(news_csv):
                self.log.info("%s 已经爬取" % news_csv)
                return
        news_url = url + "/news"  # 新闻
        self.getNews(item, news_url)
        self.log.info("获取%s成功" % item.id)
        self.pipeline_save_url(item)

    def pipeline_save_url(self, item):
        item_dir = self.save_pre_dir + item.id
        if not os.path.exists(item_dir):
            os.makedirs(item_dir)
        # 1、新闻#[title,url,date,author,source,source_url,img_url,content]
        self.save_news_csv(item, item_dir)
        self.log.info("%s写入文件成功" % (item.id))

    def save_news_csv(self, item, item_dir):
        news_csv = item_dir + '/news.csv'
        ori_len = 0
        ori_crawled = 0
        if os.path.exists(news_csv):
            ori_crawled = 1
            with open(news_csv, 'r', encoding="utf8") as fi:
                lines = fi.readlines()
                ori_len = (len(lines) - 1)
        with open(news_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["title", "url", "date", "author", "source", "img_url", "img_alt", "content"])
            if item.news != None:
                writer.writerows(item.news)
                self.sum_dict["news_samples"] += len(item.news) - ori_len
                self.sum_dict["news_attributes"] += (len(item.news) - ori_len) * 8
                self.sum_dict["data_news_crawled"] += 1 - ori_crawled

    def getNews(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        if htmlContent is None:
            self.log.error("Can't get this url: %s" % url)
            if os.path.exists(self.save_pre_dir + item.id):
                delete_dir(self.save_pre_dir + item.id)
            return
        soup = BeautifulSoup(htmlContent, 'lxml')
        button = soup.find('button', attrs={"id": "news-load-more"})
        if button != None:
            self.news_selenium(item, url)
            soup.decompose()
            return

        newses = soup.find_all('article', attrs={"class": re.compile("news-article")})
        if newses != []:
            self.log.info("%s tot news:%d" % (item.id, len(newses)))
            pool = ThreadPool(processes=self.THREADS)
            res = pool.map_async(self.getEachNews, newses)
            pool.close()
            pool.join()
            res.wait()
            if res.ready():
                if res.successful():
                    item.news = res.get()
        soup.decompose()

    def news_selenium(self, item, url):
        self.log.info("selenium:%s" % url)
        driver = webdriver.Chrome(chrome_options=self.chrome_options)  # executable_path='./chromedriver',
        driver.get(url)
        while True:
            t = True
            while t:
                check_height = driver.execute_script("return document.body.scrollHeight;")
                js = "var q=document.documentElement.scrollTop=" + str(check_height + 500)
                driver.execute_script(js)
                driver.implicitly_wait(3)
                time.sleep(3)
                check_height1 = driver.execute_script("return document.body.scrollHeight;")
                self.log.info(str(item.id) + ': ' + str(check_height) + '**************' + str(check_height1))
                if check_height == check_height1:
                    t = False
            try:
                next =driver.find_element(By.ID,'news-load-more') #driver.find_element_by_id('news-load-more')
                driver.execute_script("arguments[0].scrollIntoView();", next)
                next.click()
                driver.implicitly_wait(5)
                time.sleep(5)
            except Exception as e:
                self.log.info("%s  selenium over!(%s)" % (url, e))
            htmlcontent = driver.page_source
            soup = BeautifulSoup(htmlcontent, 'lxml')
            newses = soup.find_all('article', attrs={"class": re.compile("news-article")})
            self.log.info("%s get news:%d" % (item.id, len(newses)))
            load_all = soup.find("div", attrs={"id": "main"}).find('div', attrs={"class": re.compile("loaded-all")})
            if load_all is not None:
                self.log.info("%s  selenium over!" % url)
                soup.decompose()
                break
            soup.decompose()

        htmlcontent = driver.page_source
        soup = BeautifulSoup(htmlcontent, 'lxml')
        newses = soup.find_all('article', attrs={"class": re.compile("news-article")})
        newses.reverse()
        if newses != []:
            self.log.info("%s tot news:%d" % (item.id, len(newses)))
            pool = ThreadPool(processes=self.THREADS)
            res = pool.map_async(self.getEachNews, newses)
            pool.close()
            pool.join()
            res.wait()
            if res.ready():
                if res.successful():
                    item.news = res.get()
        driver.delete_all_cookies()
        driver.close()
        driver.quit()
        soup.decompose()

    def getEachNews(self, news):
        cur_news = []  # [title,url,date,author,source,source_url,img_url,content]
        header = news.find('header')
        if header != None:
            title = header.find('h2', attrs={"class": "news-article__title"})
            if title != None:
                cur_news.append(title.get_text().strip())
                Url = title.find('a')
                if Url != None:
                    Url = Url.attrs['href']
                    cur_news.append(Url)
                else:
                    cur_news.append("")
            else:
                cur_news.append("")
                cur_news.append("")
            date = header.find('li', attrs={"class": re.compile("news-article__date")})
            if date != None:
                cur_news.append(date.get_text().strip())
            else:
                cur_news.append("")
            author = header.find('li', attrs={"class": re.compile("news-article__author")})
            if author != None:
                cur_news.append(author.get_text().strip())
            else:
                cur_news.append("")
            source = header.find('li', attrs={"class": re.compile("news-article__source")})
            if source != None:
                cur_news.append(source.get_text().strip())
            else:
                cur_news.append("")
        else:
            cur_news.append("")
            cur_news.append("")
            cur_news.append("")
            cur_news.append("")
            cur_news.append("")
        img = news.find("img", attrs={"class": "news-article__image"})
        if img != None:
            cur_news.append(img.attrs['src'].strip())
            cur_news.append(img.attrs['alt'].strip())
        else:
            cur_news.append("")
            cur_news.append("")
        content = news.find("div", attrs={"class": "news-article__content"})
        if content != None:
            content_strings = content.strings
            tot_content = ""
            for cur_string in content_strings:
                if tot_content != "" and cur_string.strip() != "":
                    tot_content += ' '
                tot_content += cur_string.strip().replace('\n', ' ')
            cur_news.append(tot_content)
        else:
            cur_news.append("")

        return cur_news


class nmIMDB_news(supplement_news):
    def __init__(self, use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS):
        super().__init__(use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS)

    def start_spider(self):
        tot_urls = len(self.urls)
        self.log.info("total ids:%d" % tot_urls)
        epochs = tot_urls // (self.THREADS * 2)
        if tot_urls % (self.THREADS * 2):
            epochs += 1
        start_epoch = 0
        if os.path.exists(self.start_epoch_file):
            with open(self.start_epoch_file, "r", encoding="utf8") as fi:
                lines = fi.readlines()
                start_epoch = int(lines[0]) + 1

        self.start_time = time.time()
        self.end_time = self.start_time
        for i in range(start_epoch, epochs):
            self.sum_dict = {
                "news_samples": 0,
                "news_attributes": 0,
                "data_nms_news_crawled": 0,
            }
            start_id = i * (self.THREADS * 2)
            end_id = (i + 1) * (self.THREADS * 2)
            self.items = []
            self.log.error("start %d:%d--%d" % (i, start_id, end_id))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.spider, self.urls[start_id:end_id])
            pool.close()
            pool.join()
            with open(self.start_epoch_file, "w", encoding="utf8") as fi:
                fi.write(str(i))
            if i % 2 == 0:
                shutil.copy(self.start_epoch_file, self.start_epoch_file + '.bak.txt')

            json_file = "./data_nms_news_sum_nm_info.json"
            json_bak_file = "./data_nms_news_sum_nm_info_bak.json"
            if i % 2 == 0:
                shutil.copy(json_file, json_bak_file)
            with open(json_file, 'r') as load_f:
                load_dict = json.load(load_f)
            for cur_key in load_dict.keys():
                load_dict[cur_key] += self.sum_dict[cur_key]
                self.sum_dict[cur_key] = 0
            json_str = json.dumps(load_dict, indent=4)
            self.log.info("sum_info: " + json_str)
            with open(json_file, "w") as f:
                f.write(json_str)

        self.end_time = time.time()
        total_time = self.end_time - self.start_time
        self.log.info('爬取数据信息及链接完成，总用时： {:.0f}m {:.0f}s'.format(total_time // 60, total_time % 60))

    def spider(self, url):
        item = Item()
        item.id = re.search("/(nm\d+)", url).group(1).strip()
        news_csv = self.save_pre_dir + item.id + '/news.csv'
        if not self.is_repair:
            if os.path.exists(news_csv):
                self.log.info("%s 已经爬取" % news_csv)
                return
        news_url = url + "/news"  # 新闻
        htmlContent = self.isok_getUrlInfo(url)
        if htmlContent is None:
            self.log.error("Can't get this url:%s" % url)
            if os.path.exists(self.save_pre_dir + item.id):
                delete_dir(self.save_pre_dir + item.id)
            return
        self.getNews(item, news_url)
        self.log.info("获取%s成功" % item.id)
        self.pipeline_save_url(item)

    def pipeline_save_url(self, item):
        item_dir = self.save_pre_dir + item.id
        if not os.path.exists(item_dir):
            os.makedirs(item_dir)
        # 1、新闻#[title,url,date,author,source,source_url,img_url,content]
        self.save_news_csv(item, item_dir)
        self.log.info("%s写入文件成功" % (item.id))

    def save_news_csv(self, item, item_dir):
        news_csv = item_dir + '/news.csv'
        ori_len = 0
        ori_crawled = 0
        if os.path.exists(news_csv):
            ori_crawled = 1
            with open(news_csv, 'r', encoding="utf8") as fi:
                lines = fi.readlines()
                ori_len = (len(lines) - 1)
        with open(news_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["title", "url", "date", "author", "source", "img_url", "img_alt", "content"])
            if item.news != None:
                writer.writerows(item.news)
                self.sum_dict["news_samples"] += len(item.news) - ori_len
                self.sum_dict["news_attributes"] += (len(item.news) - ori_len) * 8
                self.sum_dict["data_nms_news_crawled"] += 1 - ori_crawled


class ratingsIMDB(films_spider):
    def __init__(self, use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS):
        super().__init__(use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS)

    def start_spider(self):
        tot_urls = len(self.urls)
        self.log.info("total ids:%d" % tot_urls)
        epochs = tot_urls // (self.THREADS * 2)
        if tot_urls % (self.THREADS * 2):
            epochs += 1
        start_epoch = 0
        if os.path.exists(self.start_epoch_file):
            with open(self.start_epoch_file, "r", encoding="utf8") as fi:
                lines = fi.readlines()
                start_epoch = int(lines[0]) + 1

        self.start_time = time.time()
        self.end_time = self.start_time
        for i in range(start_epoch, epochs):
            self.sum_dict = {
                "ratings_samples": 0,
                "ratings_attributes": 0,
                "data_ratings_crawled": 0,
            }
            start_id = i * (self.THREADS * 2)
            end_id = (i + 1) * (self.THREADS * 2)
            self.items = []
            self.log.error("start %d:%d--%d" % (i, start_id, end_id))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.spider, self.urls[start_id:end_id])
            pool.close()
            pool.join()
            with open(self.start_epoch_file, "w", encoding="utf8") as fi:
                fi.write(str(i))
            if i % 2 == 0:
                shutil.copy(self.start_epoch_file, self.start_epoch_file + '.bak.txt')

            json_file = "./data_ratings_sum_tt_info.json"
            json_bak_file = "./data_ratings_sum_tt_info_bak.json"
            if i % 2 == 0:
                shutil.copy(json_file, json_bak_file)
            with open(json_file, 'r') as load_f:
                load_dict = json.load(load_f)
            for cur_key in load_dict.keys():
                load_dict[cur_key] += self.sum_dict[cur_key]
                self.sum_dict[cur_key] = 0
            json_str = json.dumps(load_dict, indent=4)
            self.log.info("sum_info: " + json_str)
            with open(json_file, "w") as f:
                f.write(json_str)

        self.end_time = time.time()
        total_time = self.end_time - self.start_time
        self.log.info('爬取数据信息及链接完成，总用时： {:.0f}m {:.0f}s'.format(total_time // 60, total_time % 60))

    def spider(self, url):
        item = Item()
        item.id = re.search("/(tt\d+)", url).group(1).strip()
        news_csv = self.save_pre_dir + item.id + '/ratings.csv'
        if not self.is_repair:
            if os.path.exists(news_csv):
                self.log.info("%s 已经爬取" % news_csv)
                return
        ratings_url = url + "/ratings"  # 新闻
        self.getRatings(item, ratings_url)
        self.log.info("获取%s成功" % item.id)
        self.pipeline_save_url(item)

    def pipeline_save_url(self, item):
        item_dir = self.save_pre_dir + item.id
        if not os.path.exists(item_dir):
            os.makedirs(item_dir)
        self.save_ratings_csv(item, item_dir)
        self.log.info("%s写入文件成功" % (item.id))

    def save_ratings_csv(self, item, item_dir):
        ratings_csv = item_dir + '/ratings.csv'
        if os.path.exists(ratings_csv):
            with open(ratings_csv, 'a+', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.sep)
                if item.ratings != None:
                    writer.writerows(item.ratings)
                    self.sum_dict["ratings_samples"] += len(item.ratings)
                    self.sum_dict["ratings_attributes"] += len(item.ratings) * 8
        else:
            with open(ratings_csv, 'w', encoding="utf8", newline='') as fi:
                writer = csv.writer(fi, delimiter=self.sep)
                writer.writerow(["tot_users", "vote_weighted_average_score", "rating_by_score", "Arithmetic_mean_score",
                                 "Median_score",
                                 "rating_by_demographic", "rating_by_users", "time_stamp"])
                if item.ratings != None:
                    writer.writerows(item.ratings)
                    self.sum_dict["ratings_samples"] += len(item.ratings)
                    self.sum_dict["ratings_attributes"] += len(item.ratings) * 8
                    self.sum_dict["data_ratings_crawled"] += 1

    def getRatings(self, item, url):
        htmlContent = self.isok_getUrlInfo(url)
        if htmlContent is None:
            self.log.error("Can't get this url: %s" % url)
            if os.path.exists(self.save_pre_dir + item.id):
                delete_dir(self.save_pre_dir + item.id)
            return
        soup = BeautifulSoup(htmlContent, 'lxml')

        rating_page = soup.find('div', attrs={"class": "title-ratings-sub-page"}).find("div",
                                                                                       attrs={"class": "allText"},
                                                                                       recursive=False) \
            .find("div", attrs={"class": "allText"})
        if rating_page is None:
            return
        if 'No rating data available' in soup.find('div', attrs={"class": "title-ratings-sub-page"}).find("div", attrs={"class": "allText"}, recursive=False).contents[0]:
            return
        tot_users = re.search("([\d,\s]+)IMDb users", rating_page.contents[0]).group(1).strip().replace(',', '')
        vote_weighted_average_score = re.search("vote of([\d\.\s]+)", rating_page.contents[2]).group(1).strip()
        rating_by_score = ""
        score_table = rating_page.find_all("table")[0]
        score_table_trs = score_table.find_all('tr')[1:]
        for cur_tr in score_table_trs:
            cur_score = cur_tr.find_all("td")[0].get_text().strip()
            cur_score_ratio = cur_tr.find_all("td")[1].get_text().strip()
            cur_score_votes = cur_tr.find_all("td")[2].get_text().strip().replace(',', '')
            tmp = cur_score + ':' + cur_score_ratio + ',' + cur_score_votes
            if rating_by_score != "":
                rating_by_score += ';'
            rating_by_score += tmp
        statics = rating_page.find("div", attrs={"class": "allText"}, text=re.compile("Arithmetic mean"))
        Arithmetic_mean_score = re.search("Arithmetic mean =([\d\.\s]+)", statics.get_text().strip()).group(1).strip()
        Median_score = re.search("Median =([\d\.\s]+)", statics.get_text().strip()).group(1).strip()

        rating_by_demographic = "{"
        demographic_table = rating_page.find_all("table")[1]
        demographic_table_trs = demographic_table.find_all("tr")[1:]
        cnt = 0
        for cur_tr in demographic_table_trs:
            cate = cur_tr.find_all('td')[0].get_text().strip()
            col_cate = ['All_ages', '<18', '18-29', '30-44', '45+']
            cur_cate = "'" + cate + "':{"
            for pos in range(5):
                tmp_score = cur_tr.find_all('td')[pos + 1].find('div', attrs={"class": "bigcell"}).get_text().strip()
                if cur_tr.find_all('td')[pos + 1].find('div', attrs={"class": "smallcell"}):
                    tmp_votes = cur_tr.find_all('td')[pos + 1].find('div', attrs={
                        "class": "smallcell"}).get_text().strip().replace(',', '')
                else:
                    tmp_votes = '-'
                if pos:
                    cur_cate += ','
                cur_cate += "'" + col_cate[pos] + "'" + ':{' + "'score':" + tmp_score + ",'votes':" + tmp_votes + '}'
            cur_cate += '}'
            if cnt:
                rating_by_demographic += ','
            rating_by_demographic += cur_cate
            cnt += 1
        rating_by_demographic += '}'

        rating_by_users = ''
        users_table = rating_page.find_all("table")[2]
        users_table_tr_td = users_table.find_all('tr')[-1].find_all('td')
        user_cate = ['Top_1000_voters', 'US_users', 'Non_US_Users']
        for pos in range(3):
            tmp_score = users_table_tr_td[pos].find('div', attrs={"class": "bigcell"}).get_text().strip()
            if users_table_tr_td[pos].find('div', attrs={"class": "smallcell"}):
                tmp_votes = users_table_tr_td[pos].find('div', attrs={"class": "smallcell"}).get_text().strip().replace(
                    ',',
                    '')
            else:
                tmp_votes = '-'
            if pos:
                rating_by_users += ';'
            rating_by_users += user_cate[pos] + ':' + tmp_score + ',' + tmp_votes

        time_stamp = str(int(time.mktime(datetime.datetime.now().timetuple())))
        item.ratings = [[tot_users, vote_weighted_average_score, rating_by_score, Arithmetic_mean_score, Median_score,
                         rating_by_demographic, rating_by_users, time_stamp]]
        soup.decompose()


class nmImgTags_IMDB(films_spider):
    def __init__(self, use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS):
        super().__init__(use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS)

    def start_spider(self):
        tot_urls = len(self.urls)
        self.log.info("total ids:%d" % tot_urls)
        epochs = tot_urls // (self.THREADS * 2)
        if tot_urls % (self.THREADS * 2):
            epochs += 1
        start_epoch = 0
        if os.path.exists(self.start_epoch_file):
            with open(self.start_epoch_file, "r", encoding="utf8") as fi:
                lines = fi.readlines()
                start_epoch = int(lines[0]) + 1

        self.start_time = time.time()
        self.end_time = self.start_time
        for i in range(start_epoch, epochs):
            self.sum_dict = {
                "large_img_info_samples": 0,
                "large_img_info_attributes": 0,
                "data_nms_TagsForLargeImg_crawled": 0,
            }
            start_id = i * (self.THREADS * 2)
            end_id = (i + 1) * (self.THREADS * 2)
            self.items = []
            self.log.error("start %d:%d--%d" % (i, start_id, end_id))
            pool = ThreadPool(processes=self.THREADS)
            pool.map(self.spider, self.urls[start_id:end_id])
            pool.close()
            pool.join()
            with open(self.start_epoch_file, "w", encoding="utf8") as fi:
                fi.write(str(i))
            if i % 2 == 0:
                shutil.copy(self.start_epoch_file, self.start_epoch_file + '.bak.txt')

            json_file = "./data_nms_TagsForLargeImg_sum_nm_info.json"
            json_bak_file = "./data_nms_TagsForLargeImg_sum_nm_info_bak.json"
            if i % 2 == 0:
                shutil.copy(json_file, json_bak_file)
            with open(json_file, 'r') as load_f:
                load_dict = json.load(load_f)
            for cur_key in load_dict.keys():
                load_dict[cur_key] += self.sum_dict[cur_key]
                self.sum_dict[cur_key] = 0
            json_str = json.dumps(load_dict, indent=4)
            self.log.info("sum_info: " + json_str)
            with open(json_file, "w") as f:
                f.write(json_str)

        self.end_time = time.time()
        total_time = self.end_time - self.start_time
        self.log.info('爬取数据信息及链接完成，总用时： {:.0f}m {:.0f}s'.format(total_time // 60, total_time % 60))

    def spider(self, url):
        item = Item()
        item.id = re.search("/(nm\d+)", url).group(1).strip()
        imgtag_csv = self.save_pre_dir + item.id + '/large_img_info.csv'
        if not self.is_repair:
            if os.path.exists(imgtag_csv):
                self.log.info("%s 已经爬取" % url)
                return
        img_url = url + "/mediaindex"  # 演员相关图片
        html2 = self.isok_getUrlInfo(img_url)
        if html2 is None:
            self.log.info("Can't get this url: %s" % img_url)
            if os.path.exists(self.save_pre_dir + item.id):
                delete_dir(self.save_pre_dir + item.id)
            return
        soup2 = BeautifulSoup(html2, 'lxml')
        leftright = soup2.find('div', attrs={"id": "media_index_content", "class": "header"})
        if leftright != None:
            photos = leftright.find('div', attrs={"id": "left", "class": "desc"})
            if photos != None:
                photos = photos.get_text().strip()
                nums = re.findall('(\d+)', photos)
                if len(nums) >= 3:
                    nums = ''.join(nums[2:])
                else:
                    nums = ''.join(nums)
                nums = int(nums)
                self.getAllimgsinfo(item, img_url, nums)
        soup2.decompose()
        self.log.info('%s:imgtags is over' % item.id)
        self.log.info("获取%s成功" % item.id)
        self.pipeline_save_url(item)

    def getAllimgsinfo(self, item, img_url, nums):
        self.log.info("%s有%s张图片" % (item.id, nums))
        pages = nums // self.PER_PAGE_OF_IMG
        if nums % self.PER_PAGE_OF_IMG:
            pages += 1
        page_url = []
        orlList = []
        for i in range(1, pages + 1):
            page_url.append(img_url + '?page=' + str(i))
        pool = ThreadPool(processes=self.THREADS)
        res = pool.map_async(self.get_img_oriList, page_url)
        pool.close()
        pool.join()
        res.wait()
        if res.ready():
            if res.successful():
                orlLists = res.get()
                for i in orlLists:
                    orlList.extend(i)
        self.log.info("%s tot imgs:%d" % (item.id, len(orlList)))

        pool = ThreadPool(processes=self.THREADS)
        res = pool.map_async(self.get_ori_imgs_info, orlList)
        pool.close()
        pool.join()
        res.wait()
        if res.ready():
            if res.successful():
                tmp = res.get()
                self.log.info("%d img tags get for %d imgs" % (len(tmp), len(orlList)))
                item.large_img_tag = tmp

    def get_img_oriList(self, cur_url):
        htmlContent = self.isok_getUrlInfo(cur_url)
        soup = BeautifulSoup(htmlContent, 'lxml')
        imgs = soup.find('div', attrs={"class": "media_index_thumb_list", "id": "media_index_thumbnail_grid"})
        imgs = imgs.find_all('img')
        orlList = []
        for img in imgs:
            img_ori_url = "https://www.imdb.com" + img.parent.attrs['href']
            orlList.append(img_ori_url)
        soup.decompose()
        return orlList

    def get_ori_imgs_info(self, img_url):
        self.log.info("selenium:%s" % img_url)
        driver = webdriver.Chrome(chrome_options=self.chrome_options)
        try_cnt = 0
        while try_cnt <= 10:
            while True:
                try:
                    driver.get(img_url)
                    driver.implicitly_wait(3)
                    # driver = webdriver.Firefox(firefox_options=chrome_options)
                    break
                except Exception as e:
                    self.log.error("selenium: 获取driver失败，重试中(%s)" % e)

            try:
                button =driver.find_elements(By.CSS_SELECTOR,'button[aria-label="Open"]') #driver.find_elements_by_css_selector('button[aria-label="Open"]')
                if len(button) > 0:
                    driver.execute_script("arguments[0].scrollIntoView();", button[0])
                    button[0].click()
            except Exception as e:
                self.log.error(e)
            try:
                htmlcontent = driver.page_source
                soup = BeautifulSoup(htmlcontent, 'lxml')
                img_ori_url = soup.find('meta', attrs={"property": "og:image"})
                img_id = re.search("/(rm\d+)", img_url).group(1).strip()
                if img_ori_url is None:
                    self.log.error("%s 获取地址出错" % img_url)
                    driver.delete_all_cookies()
                    driver.close()
                    driver.quit()
                    soup.decompose()
                    return ["", "", "", "", ""]
                img_ori_url = img_ori_url.attrs['content']
                title = soup.find('title').get_text().strip()
                describe = soup.find('meta', attrs={"name": "description"}).attrs["content"].strip()
                content = soup.find('div', attrs={"class": "item-metadata"})
                # content=soup.find('div',attrs={"data-testid":"media-sheet"})
                content = content.find_all('div')
                info_list = [img_id, img_ori_url, title, describe]
                info_other = []
                for div in content:
                    type = div.find('strong')
                    if type == None:
                        continue
                    strong_text = type.get_text().strip()
                    info = div.get_text().strip()
                    info_other.append(info)
                    if re.search("People", strong_text):
                        people_string = "People_ID: "
                        people_ids = []
                        links = div.find_all('a')
                        for link in links:
                            link_url = link.attrs['href'].strip()
                            link_id = re.search("/(nm\d+)", link_url).group(1).strip()
                            people_ids.append(link_id)
                        people_string += ','.join(people_ids)
                        info_other.append(people_string)
                info_list.append('||'.join(info_other))
                soup.decompose()
                break
            except Exception as e:
                self.log.error("selenium: 获取信息失败，重试中(%s)" % e)
                try:
                    htmlcontent = driver.page_source
                    soup = BeautifulSoup(htmlcontent, 'lxml')
                    img_ori_url = soup.find('meta', attrs={"property": "og:image"})
                    img_id = re.search("/(rm\d+)", img_url).group(1).strip()
                    if img_ori_url is None:
                        self.log.error("%s 获取地址出错" % img_url)
                        driver.delete_all_cookies()
                        driver.close()
                        driver.quit()
                        soup.decompose()
                        return ["", "", "", "", ""]
                    img_ori_url = img_ori_url.attrs['content']
                    content = soup.find('div', attrs={"data-testid": "media-sheet"})
                    content = content.contents[0]
                    chs = content.contents
                    title = chs[0].contents[0].get_text().strip()
                    other = chs[1]
                    other_chs = other.contents
                    describe = other_chs[0].get_text().strip()
                    info_list = [img_id, img_ori_url, title, describe]
                    info_other = []
                    for div in other_chs[2].contents:
                        spans = div.contents
                        cur_t, cur_c = None, None
                        if len(spans) < 2:
                            cur_t = div.attrs['data-testid'].strip() + ': '
                            cur_c = div.get_text().strip()
                        else:
                            cur_t = spans[0].get_text().strip() + ': '
                            cur_c = spans[1].get_text().strip()
                        info_other.append(cur_t + cur_c)
                        if re.search("People", cur_t):
                            people_string = "People_ID: "
                            people_ids = []
                            links = spans[1].find_all('a')
                            for link in links:
                                link_url = link.attrs['href'].strip()
                                link_id = re.search("/(nm\d+)", link_url).group(1).strip()
                                people_ids.append(link_id)
                            people_string += ','.join(people_ids)
                            info_other.append(people_string)
                    info_list.append('||'.join(info_other))
                    soup.decompose()
                    break
                except Exception as e:
                    self.log.error(e)
                    try_cnt += 1

        if try_cnt > 10:
            self.log.error("获取原图信息出错：%s" % img_url)
            driver.delete_all_cookies()
            driver.close()
            driver.quit()
            return ["", "", "", "", ""]
        self.log.info("%s:selenium over!" % img_url)
        driver.delete_all_cookies()
        driver.close()
        driver.quit()
        return info_list

    def pipeline_save_url(self, item):
        item_dir = self.save_pre_dir + item.id
        if not os.path.exists(item_dir):
            os.makedirs(item_dir)
        self.save_imgtags_csv(item, item_dir)
        self.log.info("%s写入文件成功" % (item.id))

    def save_imgtags_csv(self, item, item_dir):
        large_img_info_csv = item_dir + '/large_img_info.csv'
        with open(large_img_info_csv, 'w', encoding="utf8", newline='') as fi:
            writer = csv.writer(fi, delimiter=self.sep)
            writer.writerow(["ID", "url", "title", 'decribe', 'info'])
            if item.large_img_tag != None:
                writer.writerows(item.large_img_tag)
                self.sum_dict["large_img_info_samples"] += len(item.large_img_tag)
                self.sum_dict["large_img_info_attributes"] += len(item.large_img_tag) * 5
                self.sum_dict["data_nms_TagsForLargeImg_crawled"] += 1
