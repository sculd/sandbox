# Web crawler
# Implement a crawler that outputs all the unique URLs
# (i.e. clickable links) under a root domain.  All of
# the links you output should have the same domain as the root.

# We've provided a helper function for you.

from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import threading


def get_links(url: str) -> list[str]:
    """Get all the links on a page."""
    page = requests.get(url)
    bs = BeautifulSoup(page.content, features='lxml')
    links = [link.get("href") for link in bs.findAll('a')]
    absolute_urls = [urljoin(url, link) for link in links]
    return absolute_urls


# This is the website we'll crawl
website_to_crawl = "https://andyljones.com"

cnt_print = 0


def crawl_urls(domain: str) -> list[str]:
    crawled = []

    queue = [domain]
    queued = set([domain])
    lock = threading.Lock()
    cond = threading.Condition()
    slots = 4

    def crawl_and_add_to_queue(url: str, c: int = 0):
        global cnt_print
        cnt_print += 1
        print(f"crawling url: {url}, {c=}, {cnt_print=}")

        next_urls = get_links(url)
        next_urls = list(set([next_url.split('#')[0] for next_url in next_urls]))
        for next_url in next_urls:
            #print(f"next_url: {next_url}")
            if not next_url.startswith(domain):
                continue

            with lock:
                if next_url in queued:
                    continue

                queue.append(next_url)
                queued.add(next_url)

    def loop_process(slot):
        print(f"{slot=}")
        while True:
            with cond:
                cond.wait_for(lambda: len(queue) > 0, timeout=5)
                if not queue:
                    break
                url = queue.pop()
                crawl_and_add_to_queue(url)
                cond.notify_all()

                with lock:
                    # should do the domain check
                    if url.startswith(domain):
                        crawled.append(url)

    loop_threads = []
    for slot in range(slots):
        th = threading.Thread(target=loop_process, args=[slot])
        th.start()
        loop_threads.append(th)

    for th in loop_threads:
        th.join()

    '''
    while queue:
        cnt = 4
        urls = []
        while queue and cnt > 0:
            urls.append((queue.pop(), cnt))
            cnt -= 1

        threads = []
        for url, c in urls:
            # should do the domain check
            if url.startswith(domain):
                crawled.append(url)

            threads.append(
                threading.Thread(target=crawl_and_add_to_queue, args=[url, c]))

        for t in threads:
            t.start()

        for t in threads:
            t.join()
    '''
    '''
    while queue:
        url = queue.pop()
        print(f"{url=}")
        # should do the domain check
        if url.startswith(domain):
            crawled.append(url)

        next_urls = get_links(url)
        l_orig = len(next_urls)
        next_urls = list(set([next_url.split('#')[0] for next_url in next_urls]))
        if l_orig:
            print(f"orig: {l_orig}, new: {len(next_urls)}")

        for next_url in next_urls:
            if next_url in queued:
                continue
            if not next_url.startswith(domain):
                continue

            queue.append(next_url)
            queued.add(next_url)
    '''
    return crawled


if __name__ == '__main__':
    crawled = crawl_urls(website_to_crawl)
    print(f"crawled:\n{crawled}")
    print(f"length: {len(crawled)}")
