from threading import Thread
from collections import defaultdict
from utils.download import download
from utils import get_logger
from scraper import scraper
import time


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.unique = set()
        self.freq = {}
        self.longest_page = {}
        self.subdomains = defaultdict(set)
        super().__init__(daemon=True)

    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            resp = download(tbd_url, self.config, self.logger)
            if not resp.raw_response or resp.status != 200:
                continue
            if resp.raw_response.headers.get("content-type") != 'text/html; charset=UTF-8':
                continue
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper(
                tbd_url, resp, self.unique, self.freq, self.longest_page, self.subdomains)
            try:
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
                self.frontier.mark_url_complete(tbd_url)
            except TypeError:
                print("TypeError for", tbd_url)
            time.sleep(self.config.time_delay)

        f = open("report.txt", "w")

        f.write("1. Unique pages:" + str(len(self.unique)) + "\n")

        f.write("2. Longest page:" +
                str(max(self.longest_page, key=self.longest_page.get)) + "\n")

        f.write("3. 50 Most Frequent words" + "\n")
        s = {k: v for k, v in sorted(
            self.freq.items(), key=lambda item: item[1], reverse=True)}
        count = 0
        for i, word in enumerate(s):
            if count == 50:
                break
            f.write(str(i+1) + ". " + word + "\n")
            count += 1
        f.write("4. Subdomains" + "\n")
        for k in sorted(self.subdomains.keys()):
            f.write(str(k) + " : " + str(len(self.subdomains[k])) + "\n")
