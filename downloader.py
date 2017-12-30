"""
MIT License

Copyright (c) 2017 Daniel N. R. da Silva

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""


from abc import abstractmethod
from bs4 import BeautifulSoup
import click
import os
import urllib3
import urllib
from logger import *
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class NetworkDownloader(object):
    """
    This class implements a network (graphs and respective meta data) downloader.
    """

    def __init__(self, repository_name, site_url):
        """
        
        :param repository_name: 
        :param site_url: 
        :param headers: 
        """

        self._repository_name = repository_name
        self._site_url = site_url
        self._headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0'}
        self._downloadable_urls = []

    @property
    def downloadable_urls(self):
        if not len(self._downloadable_urls):
            self.get_urls()

        return sorted(set(self._downloadable_urls))

    @timer_decorator('download networks')
    def download_networks(self, repository_output_dir):
        """
        
        :param repository_output_dir: 
        :return: 
        """

        for url in self.downloadable_urls:
            url_out_filename = os.path.join(repository_output_dir, urllib.parse.urlparse(url).path.split('/')[-1])
            self._download_network(url_out_filename, url)

    def _download_network(self, out_filename, target_url):
        """
        
        :param out_filename: 
        :param target_url: 
        :return: 
        """

        http = urllib3.PoolManager()
        chunk_size = 12 * 1024
        try:
            request = http.urlopen('GET', target_url, headers=self._headers, preload_content=False)
            total_size = int(request.getheaders()['Content-Length'])

            with click.progressbar(length=total_size,
                                   label='Downloading {}'.format(os.path.basename(out_filename))) as bar:
                with open(out_filename, 'wb') as out_file:
                    while True:
                        data = request.read(chunk_size)
                        if not data:
                            break

                        out_file.write(data)
                        bar.update(chunk_size)

            request.release_conn()

        except urllib3.exceptions.RequestError:
            pass

    @abstractmethod
    def _parse_urls_in_main_page(self, soup: BeautifulSoup) -> list:
        """
        If the network repository main page does not contain direct links to download network data, but links to pages
        where one may download the network data, this function parses the main page and get those network page links.
             
        :param soup: The repository main page "souped" html page.
        :return: A list of urls, each of which referencing a network data page.
        """

        print('You must implement this method.')
        return []

    @abstractmethod
    def _parse_urls(self, soup: BeautifulSoup):
        """
        This function reads a 'souped' page and extract the links to download network data.
        
        :param soup: The repository "souped" html page. 
        :return: A list of downloadable networks urls.
        """

        print('You must implement this method.')

    def get_urls(self):
        """
        Gets the urls to data for a repository.

        
        :return: A list of urls
        """

        try:
            http = urllib3.PoolManager()
            request = http.urlopen('GET', self._site_url, headers=self._headers)
            soup = BeautifulSoup(request.data, 'lxml')
            urls_from_main_page = self._parse_urls_in_main_page(soup)

            if urls_from_main_page is not None:
                for url in urls_from_main_page:
                    try:
                        url_request = http.urlopen('GET', url, headers=self._headers)
                        soup = BeautifulSoup(url_request.data, 'lxml')
                        self._parse_urls(soup)
                    except urllib3.exceptions.RequestError:
                        pass
            else:
                self._parse_urls(soup)

        except urllib3.exceptions.LocationValueError:
            self._parse_urls()

        except urllib3.exceptions.RequestError:
            raise


class CommonCrawlDownloader(NetworkDownloader):
    """
    This class implements a downloader for Common Crawl Network Data Set Collection. For more information,
    please visit https://www.bigdatanews.datasciencecentral.com/profiles/blogs/
    big-data-set-3-5-billion-web-pages-made-available-for-all-of-us.
    """

    def _parse_urls_in_main_page(self, soup: BeautifulSoup):
        return None

    def _parse_urls(self, soup: BeautifulSoup):
        self._downloadable_urls = [a.get('href') for td in soup.findAll('table')
                                   for a in td.findAll('a') if 'gz' in a.get('href')]


class DBLPDownloader(NetworkDownloader):
    """
    This class implements a downloader for DBLP database. For more information, please visit
    http://kdl.cs.umass.edu/databases/dblp-data.xml.gz
    """

    def _parse_urls_in_main_page(self, soup: BeautifulSoup):
        return None

    def _parse_urls(self, soup: BeautifulSoup=None):
        self._downloadable_urls = ['http://kdl.cs.umass.edu/databases/dblp-data.xml.gz']


class Dimacs11Downloader(NetworkDownloader):
    """
    This class implements a downloader for DIMACS 11 graph collection. For more information, please visit
    http://dimacs11.zib.de/.
    """

    def _parse_urls_in_main_page(self, soup: BeautifulSoup):
        return None

    def _parse_urls(self, soup: BeautifulSoup=None):
        urls = []
        for a in soup.findAll('a'):
            if a is not None and a.get('href') is not None:
                if 'instances' in a.get('href'):
                    url_parser = urllib.parse.urlparse(self._site_url)
                    url = '{}://{}/{}'.format(url_parser.scheme, url_parser.netloc, a.get('href'))
                    urls.append(url)

        self._downloadable_urls = urls


class Dimacs9Downloader(NetworkDownloader):
    """
    This class implements a downloader for DIMACS 9 graph collection. For more information, please visit
    http://www.dis.uniroma1.it/challenge9/
    """

    def _parse_urls_in_main_page(self, soup: BeautifulSoup):
        return None

    def _parse_urls(self, soup: BeautifulSoup=None):
        urls = []
        for a in soup.findAll('a'):
            if a is not None and a.get('href') is not None:
                if 'data' in a.get('href') and '.' in a.get('href'):
                    url_parser = urllib.parse.urlparse(self._site_url)
                    url = '{}://{}/challenge9/{}'.format(url_parser.scheme, url_parser.netloc, a.get('href'))
                    urls.append(url)

        self._downloadable_urls = urls


class DOIDownloader(NetworkDownloader):
    """
    This class implements a downloader for DOI urls. For more information, please visit
    https://archive.org/details/doi-urls
    """

    def _parse_urls_in_main_page(self, soup: BeautifulSoup):
        return None

    def _parse_urls(self, soup: BeautifulSoup):
        self._downloadable_urls = ['https://archive.org/compress/doi-urls/formats=' +
                                   'COMMA-SEPARATED%20VALUES%20GZ&file=/doi-urls.zip']


class HetrecDownloader(NetworkDownloader):
    """
    This class implements a downloader for HetRec 2011 data set collection. For more information, please visit
    https://grouplens.org/datasets/hetrec-2011/.
    """

    def _parse_urls_in_main_page(self, soup: BeautifulSoup):
        return None

    def _parse_urls(self, soup: BeautifulSoup):
        self._downloadable_urls = [a.get('href') for a in soup.findAll('a')
                                   if a is not None and a.get('href') is not None and 'zip' in a.get('href')]


class KoneDownloader(NetworkDownloader):
    """
    This class implements a downloader for Konect network collection. For more information, please visit
    http://konect.uni-koblenz.de/downloads/.
    """

    def _parse_urls_in_main_page(self, soup: BeautifulSoup):
        return None

    def _parse_urls(self, soup: BeautifulSoup):
        self._downloadable_urls = ['/'.join(self._site_url.split('/')[:-1]) + '/' + a.get('href')
                                   for table_sorter_child in soup.findAll('table', {'id': 'sort1'})
                                   for a in table_sorter_child.findAll('a')
                                   if 'http' not in a.get('href') and '#' not in a.get('href')
                                   and 'tsv/' in a.get('href')]


class LALGDownloader(NetworkDownloader):
    """
    This class implements a downloader for Laboratory for Web Algorithmics network collection. For more information,
    please visit http://law.di.unimi.it/datasets.php.
    """

    def _parse_urls_in_main_page(self, soup: BeautifulSoup):
        return ['http://' + self._site_url.split('/')[2] + '/' + a.get('href')
                for table_sorter_child in soup.findAll('table', {'class': 'tablesorter'})
                for a in table_sorter_child.findAll('a')]

    def _parse_urls(self, soup: BeautifulSoup):
        self._downloadable_urls += [a.get('href')
                                    for table_data_set_child in soup.findAll('table', {'class': 'dataset'})
                                    for a in table_data_set_child.findAll('a')
                                    ][0:2]


class MVLensDownloader(NetworkDownloader):
    """
    This class implements a downloader for MovieLens data set. For more information, please visit https://grouplens.org/
    datasets/movielens/    
    """

    def _parse_urls_in_main_page(self, soup: BeautifulSoup):
        return None

    def _parse_urls(self, soup: BeautifulSoup):
        self._downloadable_urls = [a.get('href') for a in soup.findAll('a')
                                   if a is not None and a.get('href') is not None and 'zip' in a.get('href')]


class NBERDownloader(NetworkDownloader):
    """
    This class implements a downloader for NBER data set. For more information, please visit http://nber.org/patents/.
    """

    def _parse_urls_in_main_page(self, soup: BeautifulSoup):
        return None

    def _parse_urls(self, soup: BeautifulSoup):
        self._downloadable_urls = ['/'.join(self._site_url.split('/')[:-2]) + a.get('href')
                                   for table_sorter_child in soup.findAll('table')
                                   for a in table_sorter_child.findAll('a') if 'zip' in a.get('href')]


class NetworkRepositoryDownloader(NetworkDownloader):
    """
    This class implements a downloader for Network Repository network data set. For more information, please visit
    http://networkrepository.com.
    """

    def _parse_urls_in_main_page(self, soup: BeautifulSoup):
        return None

    def _parse_urls(self, soup: BeautifulSoup):
        self._downloadable_urls = [link.get('href')
                                   for link in soup.table.findAll('a') if 'php' not in link.get('href')]


class SmallDownloader(NetworkDownloader):
    """
    This class implements a downloader for Mark Newman's network collection. For more information, please visit
    http://www-personal.umich.edu/~mejn/netdata/.
    """

    def _parse_urls_in_main_page(self, soup: BeautifulSoup):
        return None

    def _parse_urls(self, soup: BeautifulSoup):
        self._downloadable_urls = [self._site_url + a.get('href') for ul in soup.findAll('ul', limit=1)
                                   for a in ul.findAll('a') if 'zip' in a.get('href')]


class SNAPDownloader(NetworkDownloader):
    """
    This class implements a downloader for the Stanford Large Network Data Set Collection. For more information,
    please visit https://snap.stanford.edu/data/.
    """

    def _parse_urls_in_main_page(self, soup: BeautifulSoup):
        url_getter_logger.info('Getting urls.')

        return ['/'.join(self._site_url.split('/')[:-1]) + '/' + a.get('href')
                for table_sorter_child in soup.findAll('table', {'id': 'datatab2'})
                for a in table_sorter_child.findAll('a')
                if 'http' not in a.get('href') and '#' not in a.get('href')]

    def _parse_urls(self, soup: BeautifulSoup):
        self._downloadable_urls += ['/'.join(self._site_url.split('/')[:-1]) + '/' + a.get('href').replace('../data/', '')
                                    for table_data_set_child in soup.findAll('table', {'id': 'datatab'})
                                    for a in table_data_set_child.findAll('a')]


class SparseMatrixDownloader(NetworkDownloader):
    """
    This class implements a downloader for the University of Florida Sparse Matrices Collection. For more information,
    please visit https://sparse.tamu.edu/.
    """

    def _parse_urls_in_main_page(self, soup: BeautifulSoup):
        return None

    def _parse_urls(self, soup: BeautifulSoup):
        self._downloadable_urls = ['/'.join(self._site_url.split('/')[:-2]) + a.get('href').replace('..', '')
                                   for table_sorter_child in soup.findAll('table')
                                   for a in table_sorter_child.findAll('a')
                                   if '/MM/' in a.get('href')]

