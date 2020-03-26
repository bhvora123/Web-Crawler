__author__ = 'bhavinvora'


# URL Canonicalization:
# Converted the scheme and host to lower case
# Removed port 80 from http URLs, and port 443 from HTTPS URLs
# Removed the fragment, which begins with #
# Removed duplicate slashes

from urlparse import urlparse, urlunparse, urljoin
import re
from urllib import unquote
from datetime import datetime

collapse_url = re.compile('([^/]+/\.\./?|/\./|//|/\.$|/\.\.$)')

# method to clean the URL
def canonURL(url, parent_domain):

    (scheme, netloc, path, parameters, query, fragment) = urlparse(url)

    parent = urlparse(parent_domain)

    if not netloc and not path:
        return ""

    if not netloc and path.startswith("."):
        new_url = urljoin(parent_domain, path)
        (scheme, netloc, path, parameters, query, fragment) = urlunparse(new_url)

    elif not netloc and path:
        netloc = parent.netloc

    netloc_lower = netloc.lower()
    netloc = netloc_lower.split(":")[0]

    prev_path = path
    print prev_path
    while 1:
        path = collapse_url.sub('/', path, 1)
        print path
        if prev_path == path:
            break
        prev_path = path

    path = unquote(path)

    canon_url = urlunparse((scheme, netloc, path, "", "", ""))
    return canon_url


def cleanURL(url):
    (scheme, netloc, path, parameters, query, fragment) = urlparse(url)

    scheme = scheme.lower()
    netloc = netloc.lower()

    if scheme == "http":
        fields = netloc.split(":")
        if fields.__len__() > 1:
            if fields[1] == "80":
                netloc = fields[0]

    if scheme == "https":
        print scheme

        fields = netloc.split(":")
        if fields.__len__() > 1:
            if fields[1] == "443":
                netloc = fields[0]

    prev_path = path
    while 1:
        path = collapse_url.sub('/', path, 1)
        if prev_path == path:
            break
        prev_path = path

    path = unquote(path)
    canon_url = urlunparse((scheme, netloc, path, "", "", ""))
    print canon_url
    return canon_url

def baseURL(url):
    value = urlparse(url)
    return value.scheme + '://' + value.netloc + '/'
