__author__ = 'bhavinvora'


# A Web Crawler used to crawl atleast 20,000 Internet documents including your seed URLs, to construct a document collection
# focused on a particular topic.
# This crawler conforms strictly to a particular politeness policy.
# Once the documents are crawled, these are pooled together.
# Choose the next URL to crawl from your frontier using a best-first strategy.
# Find all outgoing links on the pages you crawl, canonicalize them, and add them to your frontier if they are new.
# For each page crawled, the following is filed with ElasticSearch : an id, the URL, the HTTP headers,
# the page contents cleaned (with term positions), the raw html, and a list of all in-links (known)
# and out-links for the page.


from datetime import datetime
import robotparser
import hashlib
from createESindex import create_index
from elasticsearch import Elasticsearch
from CanonicalizeURL import cleanURL, baseURL, canonURL
import requests
from urlparse import urljoin
import heapq
import socket
from operator import itemgetter
import urllib2
import json
from bs4 import BeautifulSoup
import time
import sys


cleanedurl1, cleanedurl2, cleanedurl3, cleanedurl4, cleanedurl5, cleanedurl6, cleanedurl7 = "", "", "", "", "", "", ""

# initialize the seeds
def __init__():

    seed1 = "http://en.wikipedia.org/wiki/College_of_Cardinals"
    cleanedurl1 = cleanURL(seed1)
    seed2 = "http://www.usccb.org/beliefs-and-teachings"
    cleanedurl2 = cleanURL(seed2)
    seed3 = "http://christianity.about.com/od/denominations/a/catholichistory.htm"
    cleanedurl3 = cleanURL(seed3)
    seed4 = "https://www.cctwincities.org/education-advocacy/catholic-social-teaching"
    cleanedurl4 = cleanURL(seed4)
    seed5 = "http://www.aboutcatholics.com/beliefs/the-basis-of-catholic-belief"
    cleanedurl5 = cleanURL(seed5)
    seed6 = "http://www.religionfacts.com/christianity"
    cleanedurl6 = cleanURL(seed6)
    seed7 = "http://history-world.org/a_history_of_the_catholic_church.htm"
    cleanedurl7 = cleanURL(seed7)

    return cleanedurl1, cleanedurl2, cleanedurl3, cleanedurl4, cleanedurl5, cleanedurl6, cleanedurl7

if __name__ == "__main__":
    start_time = datetime.now()

    reload(sys)
    sys.setdefaultencoding('utf-8')

    s = socket.setdefaulttimeout(60)

    es = Elasticsearch()

    create_index(es)

    rp = robotparser.RobotFileParser()

    cleanedurl1, cleanedurl2, cleanedurl3, cleanedurl4, cleanedurl5, cleanedurl6, cleanedurl7 = __init__()

    print "returned:::", cleanedurl1, cleanedurl2, cleanedurl3, cleanedurl4, cleanedurl5, cleanedurl6, cleanedurl7

    visited_file = "visited_urls_vishal.txt"
    f = open(visited_file, 'w')

    # initialize the frontier1 using heapq - priority queue
    # heapq has the list of URL number, inlink count and the URL itself
    frontier1 = []
    heapq.heappush(frontier1, [0, 1, cleanedurl1])
    heapq.heappush(frontier1, [0, 2, cleanedurl2])
    heapq.heappush(frontier1, [0, 3, cleanedurl3])
    heapq.heappush(frontier1, [0, 4, cleanedurl4])
    heapq.heappush(frontier1, [0, 5, cleanedurl5])
    heapq.heappush(frontier1, [0, 6, cleanedurl6])
    heapq.heappush(frontier1, [0, 7, cleanedurl7])

    frontier2 = []
    #lists to maintain the urls being processed
    frontierlist1 = []
    frontierlist2 = []

    frontierlist1.append(cleanedurl1)
    frontierlist1.append(cleanedurl2)
    frontierlist1.append(cleanedurl3)
    frontierlist1.append(cleanedurl4)
    frontierlist1.append(cleanedurl5)
    frontierlist1.append(cleanedurl6)
    frontierlist1.append(cleanedurl7)

    count = 7  #as there are seven seeds
    visited = []
    #crawl_count = 0

    inlinks = {}

    prev = ""

    #eliminate unwanted extensions
    invalid_ext = ('.7s', '.apk', 'avi', '.css', '.gif', '.jpg', '.jpeg', '.js', '.mp3',
                  '.mp4', '.mpeg', '.pdf', '.png', '.swf', '.tar', '.tif', '.tiff', '.zip', '.pl', '.exe')

    #distribute URLs coming from seeds to alternate levels
    while visited.__len__() < 22000 and (frontier1 or frontier2):

        print "frontier1 earlier", str(frontier1.__len__())

        frontier1 = heapq._nsmallest(10000, frontier1)
        if frontier1:
            frontierlist1 = list(zip(*frontier1)[2])

        print "frontier1 after", str(frontier1.__len__())

        #frontier 1 starts here
        while frontier1 and visited.__len__() < 22000:
            try:
                url_list = heapq.heappop(frontier1)
                #print "url_list::", url_list
                url = url_list.pop(2)
                url_id = hashlib.md5(url).hexdigest()

                inlinks_count = url_list.pop(0)
                frontierlist1.remove(url)
                parent_domain = baseURL(url)

                try:
                    if prev != parent_domain:
                        #rp.set_url(urlparse.urljoin(parent_domain, "robots.txt"))
                        robot_file = parent_domain + "robots.txt"
                        rp.set_url(robot_file)
                        prev = parent_domain
                        rp.read()

                except Exception, e:
                    print "robot parse exception frontier1", e
                    continue

                try:
                    print "wave1 url::", url
                    if not url.endswith(invalid_ext):
                        html_head = requests.get(url)

                except Exception, e:
                    print "unable to get the http header frontier1::", e

                if not 'content-type' in html_head.headers:
                    continue

                elif "html" in html_head.headers['content-type']:
                    try:
                        time.sleep(1)
                        raw_html = urllib2.urlopen(url).read()
                        #raw_html = unicode(html, errors="ignore")

                        # soup, raw_html, cleaned_text = parseHTML(url)
                    except Exception, e:
                        print "unable to parse html using soup frontier1", e

                    if not url in visited:
                        visited.append(url)
                        f.write(url + "\n")

                    #print "wave 1 url:" + url + " inlinks_count::" + str(inlinks_count)

                    soup = BeautifulSoup(raw_html)
                    for script in soup(["script", "style"]):
                        script.extract()

                    cleaned_text = soup.get_text()
                    page_title = soup.title.string
                    #print "Page title", page_title

                    outlinks = set()

                    for links in soup.findAll('a'):
                        complete_url = urljoin(url, links.get('href'))
                        #print "complete url:",  complete_url
                        canon_url = ""

                        try:
                            canon_url = canonURL(complete_url, url).encode("utf-8")
                        except:
                            canon_url = canonURL(complete_url, url)

                        if canon_url == "":
                            continue

                        if canon_url in visited:
                            outlinks.add(canon_url)
                            if canon_url in inlinks:
                                inlinks[canon_url].append(url)
                            else:
                                inlinks[canon_url] = [url]
                            continue

                        elif canon_url in frontierlist1:
                            try:
                                frontier1[map(itemgetter(2), frontier1).index(canon_url)][0] -= 1
                                outlinks.add(canon_url)
                                if canon_url in inlinks:
                                    inlinks[canon_url].append(url)
                                else:
                                    inlinks[canon_url] = [url]

                            except Exception, e:
                                print "if canon_url not in list1 frontier1", e
                            continue

                        elif canon_url in frontierlist2:
                            try:
                                frontier2[map(itemgetter(2), frontier2).index(canon_url)][0] -= 1
                                outlinks.add(canon_url)
                                if canon_url in inlinks:
                                    inlinks[canon_url].append(url)
                                else:
                                    inlinks[canon_url] = [url]

                            except Exception, e:
                                print "if canon_url not in list2 frontier1", e
                            continue

                        try:
                            if not rp.can_fetch("*", canon_url):
                                continue

                        except e:
                            print "Unable to fetch from robots frontier1", e
                            continue

                        if frontier2.__len__() < 22000 and not canon_url in visited:
                            count += 1
                            newURL_list = [-1, count, canon_url]
                            heapq.heappush(frontier2, newURL_list)
                            frontierlist2.append(canon_url)
                            inlinks[canon_url] = [url]

                #index the url
                output = es.index(index="vishal_in", doc_type="documents", id=url_id,
                              body={
                                  "docno": canon_url,
                                  "title": page_title,
                                  "HTTPheader": str(html_head.headers),
                                  "clean_text": cleaned_text,
                                  "raw_html": unicode(raw_html, errors="ignore"),
                                  "inlinks": list(set(inlinks[url])),
                                  "outlinks": list(outlinks),
                                  "author": "vishal",
                                  "url": url
                              })
                print "indexed....frontier1:::", count

            except Exception, e:
                print "frontier1 exception",e
                continue

            total_time = datetime.now() - start_time
            print total_time

        ####################################################
        # frontier 2 code

        print "frontier2 earlier::", str(frontier2.__len__())
        frontier2 = heapq.nsmallest(10000, frontier2)

        if frontier2:
            frontierlist2 = list(zip(*frontier2)[2])
        print "frontier2 after::", str(frontier2.__len__())

        while frontier2 and visited.__len__() < 22000:
            try:
                url_list = heapq.heappop(frontier2)
                #print "url_list::", url_list
                url = url_list.pop(2)
                url_id = hashlib.md5(url).hexdigest()

                inlinks_count = url_list.pop(0)
                frontierlist2.remove(url)
                parent_domain = baseURL(url)

                try:
                    if prev != parent_domain:
                        #rp.set_url(urlparse.urljoin(parent_domain, "robots.txt"))
                        robot_file = parent_domain + "robots.txt"
                        rp.set_url(robot_file)
                        prev = parent_domain
                        rp.read()

                except Exception, e:
                    print "robot parse exception frontier2", e

                try:
                    print "wave2 url:", url
                    if not url.endswith(invalid_ext):
                        html_head = requests.get(url)

                except Exception, e:
                    print "unable to get the http header frontier2::", e

                if not 'content-type' in html_head.headers:
                    continue

                elif "html" in html_head.headers['content-type']:
                    try:
                        time.sleep(1)
                        raw_html = urllib2.urlopen(url).read()
                        #raw_html = unicode(html, errors="ignore")
                    except Exception, e:
                        continue

                    if not url in visited:
                        visited.append(url)
                        f.write(url + "\n")

                    #print "wave 2 url:" + url + "inlinks count::" + str(inlinks_count)

                    soup = BeautifulSoup(raw_html)
                    for script in soup(["script", "style"]):
                        script.extract()

                    cleaned_text = soup.get_text()
                    page_title = soup.title.string

                    outlinks = set()

                    for links in soup.findAll('a'):
                        complete_url = urljoin(url, links.get('href'))
                        #print "complete url:", complete_url
                        canon_url = ""

                        try:
                            canon_url = canonURL(complete_url, url).encode("utf-8")
                        except:
                            canon_url = canonURL(complete_url, url)

                        if canon_url == "":
                            continue

                        try:
                            if not rp.can_fetch("*", canon_url):
                                continue
                        except:
                            continue

                        if canon_url in visited:
                            outlinks.add(canon_url)
                            if canon_url in inlinks:
                                inlinks[canon_url].append(url)
                            else:
                                inlinks[canon_url] = [url]
                            continue

                        elif canon_url in frontierlist2:
                            try:
                                frontier2[map(itemgetter(2), frontier2).index(canon_url)][0] -= 1
                                outlinks.add(canon_url)
                                if canon_url in inlinks:
                                    inlinks[canon_url].append(url)
                                else:
                                    inlinks[canon_url] = [url]

                            except Exception, e:
                                print "if canon_url not in list2 frontier2", e
                            continue

                        elif canon_url in frontierlist1:
                            try:
                                frontier1[map(itemgetter(2), frontier1).index(canon_url)][0] -= 1
                                outlinks.add(canon_url)
                                if canon_url in inlinks:
                                    inlinks[canon_url].append(url)
                                else:
                                    inlinks[canon_url] = [url]

                            except Exception, e:
                                print "if canon_url not in list1 frontier1", e
                            continue

                        if frontier1.__len__() < 22000 and not canon_url in visited:
                            count += 1
                            newURL_list = [-1, count, canon_url]
                            heapq.heappush(frontier1, newURL_list)
                            frontierlist1.append(canon_url)
                            inlinks[canon_url] = [url]

                #print "encoding::", sys.getdefaultencoding()
                #index the url
                output = es.index(index="vishal_in", doc_type="documents", id=url_id,
                              body={
                                  "docno": canon_url,
                                  "title": page_title,
                                  "HTTPheader": str(html_head.headers),
                                  "clean_text": cleaned_text,
                                  "raw_html": unicode(raw_html,errors="ignore"),
                                  "inlinks": list(set(inlinks[url])),
                                  "outlinks": list(outlinks),
                                  "author": "vishal",
                                  "url": url
                              })
                print "frontier2::indexed", count

            except Exception, e:
                print "frontier2 exception::", e
                continue

    total_time = datetime.now() - start_time
    print total_time

    print "length of visited list::", visited.__len__()
    #print visited

    f.close()

    with open("inlink_file.json", "w") as out:
        json.dump(inlinks, out)







