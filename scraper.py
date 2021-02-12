import re
from urllib.parse import urlparse, urldefrag
from bs4 import BeautifulSoup, SoupStrainer
from simhash import Simhash
from stop_words import get_stop_words


def scraper(url, resp, unique, freq, longest_page, subdomains):
    try:
        if resp.status != 200:
            return []
        url = urldefrag(url.strip())[0]
        host = urlparse(url)
        if host.query:
            return []
        links = extract_next_links(url, resp)
        unique.add(host.netloc)

        for link in links:
            # process links
            de = urlparse(link)
            if ".ics.uci.edu" in de.netloc and de.netloc != host.netloc:
                key = de.netloc
                path = de.path
                subdomains[key].add(path)
            if de.netloc:
                unique.add(de.netloc)

        stop_words = get_stop_words('english')
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        for script in soup(["script", "style"]):
            script.extract()
        text = soup.body.get_text(separator=' ')
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip()
                  for line in lines for phrase in line.split("  "))
        text = [chunk for chunk in chunks if chunk and chunk.lower()
                not in stop_words]
        # longest, account for stop words
        total_words = len(text)
        longest_page[urlparse(url).netloc] = longest_page.get(
            url, 0) + total_words

        for line in text:
            tokens = [token for token in re.split(
                '[^a-zA-Z]', line) if token != '']
            for token in tokens:
                if token.isalnum() and token.lower() not in stop_words and len(token) >= 3:
                    freq[token.lower()] = freq.get(token.lower(), 0) + 1
        return [link for link in links if is_valid(link)]
    except:
        print("Something went wrong with", url)


def extract_next_links(url, resp):
    if resp.status != 200 or not resp.raw_response:
        return []
    res = set()
    this_path = urlparse(url).path
    this_scheme = urlparse(url).scheme
    html = resp.raw_response.content
    for link in BeautifulSoup(html, parse_only=SoupStrainer('a'), features='html.parser'):
        if link.has_attr('href'):
            # clean up links
            site = ""
            if link['href'] == '/':  # same site
                continue
            if link['href'].startswith("//"):  # same domain
                site = link['href'][2:]
            elif link['href'].startswith('/') and urlparse(url).netloc not in link['href']:
                site = url + link['href']
            else:
                site = link['href']

            if not urlparse(site).scheme:
                site = this_scheme + "://" + site
            defrag_site = urldefrag(site.strip())[0]
            new_path = urlparse(defrag_site).path
            if Simhash(this_path).distance(Simhash(new_path)) > 16:
                res.add(defrag_site)
    return list(res)


def is_valid(url):
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]) or parsed.query:
            return False
        if '.php' in parsed.path or '.ppsx' in parsed.path or '.z' in parsed.path or '.war' in parsed.path:
            return False
        return any([domain in parsed.netloc for domain in
                    ['.ics.uci.edu', '.cs.uci.edu', '.informatics.uci.edu', '.stat.uci.edu',
                     'today.uci.edu/department/information_computer_sciences']]) and not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print("TypeError for ", parsed)
        raise
