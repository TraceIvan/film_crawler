# film_crawler
针对IMDB电影平台，基于Beautifulsoup搭建跨模态电影数据采集框架，获取IMDB网站上电影、演员相关的文本、图片和视频内容。
## 爬虫说明
films_spider：负责电影以下内容爬取：外部网站评论（externalreview）、导演编剧演员（fullcredits）、图片链接（mediaindex）、视频链接（videogallery）、总结梗概（plotsummary）、关键词（keywords）、家长指导（parentalguide）、发布信息（releaseinfo）、公司信息（companycredits）、拍摄地点（locations）、技术指标（technical）、琐事（trivia）、电影中出现的一些常识错误（goofs）、crazycredits、引用（quotes）、其他版本（alternateversions）、其他电影联系（movieconnections）、原声带（soundtrack）、faq、获奖信息（awards）

supplement_reviews：负责电影评论爬取

supplement_news：负责电影相关新闻爬取

imgTags_IMDB：负责电影图片相关文本tags爬取

ratingsIMDB：负责电影评分爬取

nmIMDB：负责演员以下内容爬取：获奖信息（awards）、图片链接（mediaindex）、视频链接（videogallery）、参演电影（Filmography）、生平（bio）、其他（otherworks）、报道（publicity）、外部网站（external_sites）

nmIMDB_news：负责演员相关新闻爬取

main_nmIMDB_imgtags：负责演员图片相关文本tags爬取

repair_img：从演员或电影图片页面解析下载链接进行媒体下载

repair_video：从演员或电影视频页面解析下载链接进行媒体下载

## 采集流程
以上爬虫均在spiders.py文件中实现，采用beautifulsoup+selenium进行解析。各个爬虫的采集流程类似，如下：
1)	__init__初始化爬虫参数
2)	调用start_spider启动爬取。start_spider中配置线程池，将待爬取的演员或影片id列表划分epoch，从当前断点的epoch开始，调用spider爬取各个url
3)	spider中负责调度各部分爬取内容，调用一个或多个解析函数将数据保存在item中，最后调用pipeline_save_url保存在本地。
4)	pipeline_save_url将各部分内容以文件形式进行保存。

## 接口说明
__init__(self, use_csv, save_dir, epoch_file, log_file, is_reverse, is_repair, THREADS)：
use_csv：包含所有id的csv文件路径，用于确定当前爬虫的采集目标。
save_dir：指定保存爬取结果的目录。
epoch_file：指定epoch断点文件路径。若存在，则读取断点的epoch，从该epoch开始爬取；否则建立该文件，并从头开始爬取。
log_file：指定日志文件名称，代码会自动添加日期后缀。
is_reverse：为True表示从后往前爬取，否则从前往后爬取。
is_repair：为True表示对已爬取的内容进行重新爬取，以修复部分错误（具体由“if self.is_repair:”下的代码块确定）；为False则正常爬取。
THREADS：指定线程池大小，用于控制并发规模。也同时控制epoch大小，当其修改后，同时需要对断点文件中的epoch进行修改（例如，当THREADS扩大一倍时，断点文件中的epoch需缩小一倍且向下取整）。

start_spider(self)：
该方法用于外部调用，启用爬虫开始采集。

spider(self, url)：
由start_spider函数内部调用，根据指定影片的url采集其相关内容。
url：影片的url，由影片id确定，例如：https://www.imdb.com/title/tt7349950

pipeline_save_url(self, item)：
由spider函数调用，将影片的相关内容保存在本地。
item：Item类实例，包含已爬取的影片内容。

save_XXXX _info(self, item, item_dir)：
由pipeline_save_url调用，保存item中的指定内容到本地。例如save_awards_info()函数用于保存影片获奖信息到本地。
item：Item类实例，包含已爬取的影片内容。
item_dir：保存路径

get_XXX (self, item, url)：
由spider调用，用于获取影片指定内容。例如getAwards()用于对影片获奖信息页面进行解析，并保存在Item实例中。
Item：Item类实例，用于保存采集的信息。
url：待解析页面。

