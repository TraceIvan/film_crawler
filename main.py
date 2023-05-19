'''
films_spider:repair img video company awards ok, add ori_img ok,top ok,near202009 ok,title now
reviewsIMDB:repair ok，top ok, near202009 ok, title ok, title twice now
newsIMDB:repair ok,cur top ok, near202009 ok,title ok, title twice now
imgtagsIMDB: repair ok,Top ok, Near_202009 ok, title now
ratingsIMDB: titles now
nmIMDB:repair img video ok, repair awards ok, add ori_img ok, names now
nmIMDB_news: repair ok, names now
nmIMDB_imgtags: names now
change films/reviews/news threads to 8
gross and company ok
'''
import click
from config import *
from spiders import *
from utils import *
@click.group()
def cli():
    pass
@cli.command(name="main_films")
def main_films():
    spider=films_spider(use_csv['saveIMDB'],dirs["IMDB_FILMS_DIR"],start_epoches["saveIMDB"],
                        log_files['saveIMDB'],is_reverse=True,is_repair=False,THREADS=2)
    spider.start_spider()
@cli.command(name="main_reviewsIMDB")
def main_reviewsIMDB():
    spider = supplement_reviews(use_csv['reviewsIMDB'], dirs["IMDB_FILMS_REVIEWS_DIR"], start_epoches["reviewsIMDB"],
                          log_files['reviewsIMDB'],is_reverse=True,is_repair=False,THREADS=4)
    spider.start_spider()
@cli.command(name="main_supplement_reviewsIMDB")
def main_supplement_reviewsIMDB():
    spider = supplement_reviews(use_csv['reviewsIMDB'], dirs["IMDB_FILMS_REVIEWS_DIR"], "start_epoch_supplement_reviews_reverse.txt",
                          "supplement_reviewsIMDB.log",is_reverse=True,is_repair=True,THREADS=2)
    spider.start_spider()

@cli.command(name="main_newsIMDB")
def main_newsIMDB():
    spider = supplement_news(use_csv['newsIMDB'], dirs["IMDB_FILMS_NEWS_DIR"], start_epoches["newsIMDB"],
                          log_files['newsIMDB'],is_reverse=True,is_repair=False,THREADS=4)
    spider.start_spider()
@cli.command(name="main_supplement_newsIMDB")
def main_supplement_newsIMDB():
    spider = supplement_news(use_csv['newsIMDB'], dirs["IMDB_FILMS_NEWS_DIR"], "start_epoch_supplement_news_reverse.txt",
                          "supplement_newsIMDB.log",is_reverse=True,is_repair=True,THREADS=2)
    spider.start_spider()

@cli.command(name="main_imgtagsIMDB")
def main_imgtagsIMDB():
    spider = imgTags_IMDB(use_csv['imgtagsIMDB'], dirs["IMDB_FILMS_IMGS_DIR"], start_epoches["imgtagsIMDB"],
                      log_files['imgtagsIMDB'],is_reverse=True,is_repair=False,THREADS=1)
    spider.start_spider()

@cli.command(name="main_ratingsIMDB")
def main_ratingsIMDB():
    spider = ratingsIMDB(use_csv['ratingsIMDB'], dirs["IMDB_FILMS_RATINGS"], start_epoches["ratingsIMDB"],
                      log_files['ratingsIMDB'],is_reverse=True,is_repair=False,THREADS=2)
    spider.start_spider()


@cli.command(name="main_nmIMDB")
def main_nmIMDB():
    spider = nmIMDB(use_csv['nmIMDB'], dirs["IMDB_NM_DIR"], start_epoches["nmIMDB"],
                          log_files['nmIMDB'], is_reverse=False,is_repair=False,THREADS=2)
    spider.start_spider()
@cli.command(name="main_nmIMDB_news")
def main_nmIMDB_news():
    spider = nmIMDB_news(use_csv['nmIMDB_news'], dirs["IMDB_NM_NEWS_DIR"], start_epoches["nmIMDB_news"],
                          log_files['nmIMDB_news'],is_reverse=False,is_repair=False,THREADS=2)
    spider.start_spider()
@cli.command(name="main_nmIMDB_imgtags")
def main_nmIMDB_imgtags():
    spider = nmImgTags_IMDB(use_csv['nmImgtagsIMDB'], dirs["IMDB_NM_IMGSINFO_DIR"], start_epoches["nmImgtagsIMDB"],
                          log_files['nmImgtagsIMDB'],is_reverse=False,is_repair=False,THREADS=2)
    spider.start_spider()
@cli.command(name="main_downloadIMDB_img_nm")
def main_downloadIMDB_img_nm():
    spider=downloads('nm','download_img_nm.log',THREADS=8)
@cli.command(name="main_downloadIMDB_img_tt")
def main_downloadIMDB_img_tt():
    spider=downloads('tt','download_img_tt.log',THREADS=8)
@cli.command(name="main_downloadIMDB_img_tt_new")
def main_downloadIMDB_img_tt_new():
    spider=repair_img('tt',dirs["IMDB_FILMS_DIR"],'start_epoch_repair_download_img_tt_new.txt','repair_download_img_tt_new.log',is_reverse=True,THREADS=2)
    spider.start_spider()
@cli.command(name="main_downloadIMDB_video_nm")
def main_downloadIMDB_video_nm():
    spider=repair_video('nm',dirs["IMDB_NM_DIR"],'start_epoch_repair_download_video_nm.txt','repair_download_video_nm.log',is_reverse=False,THREADS=4)
    spider.start_spider()
@cli.command(name="main_downloadIMDB_video_tt")
def main_downloadIMDB_video_tt():
    spider=repair_video('tt',dirs["IMDB_FILMS_DIR"],'start_epoch_repair_download_video_tt_tot.txt','repair_download_video_tt_tot.log',is_reverse=True,THREADS=2)
    spider.start_spider()


@cli.command(name="main_repairImg")
def main_repairImg():
    spider=repairImg('tt',THREADS=4)
@cli.command(name="main_companyIMDB")
def main_companyIMDB():
    spider=getComanies(dirs['IMDB_COS_DIR'],log_files["cosIMDB"],THREADS=4)
@cli.command(name="main_wrongcsv")
def main_wrongcsv():
    repair=wrongcsv()
@cli.command(name="main_get404")
def main_get404():
    get_csv=get_404_ttnm()
@cli.command(name="main_get_id_list")
def main_get_id_list():
    from utils import get_cur_id_list
    get_cur_id_list(dirs['IMDB_FILMS_DIR'],'tt','repair_tt.csv')
@cli.command(name="main_repair")
def main_repair():
    spider=films_spider(use_csv['saveIMDB_repair'],dirs["IMDB_FILMS_DIR"],start_epoches["saveIMDB_repair"],
                        log_files['saveIMDB_repair'],is_reverse=True,is_repair=True,THREADS=4)
    spider.start_spider()

@cli.command(name="main_movie_list_require")
def main_movie_list_require():
    spider = films_spider(use_csv['ttIMDB_require'], dirs["IMDB_FILMS_DIR"], start_epoches["ttIMDB_require"],
                          log_files['ttIMDB_require'], is_reverse=True, is_repair=False, THREADS=2)
    spider.start_spider()
if __name__=="__main__":
    cli()