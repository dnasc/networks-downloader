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


import argparse
import json
import sys

from downloader import *


if sys.version_info[0] < 3:
    raise Exception("Python 3 or a more recent version is required.")


def get_args(repositories_options_dict):
    """
    Parses the arguments entered by the user.

    :param repositories_options_dict: A dictionary for the repositories. Keys in the first level of the dict are
    repositories entries.
    :return: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description='Downloader for network data sets.',
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('-odir', help='Output directory.', required=True)

    total_spaces_to_description = 15
    parser.add_argument('-repo',
                        choices=[su for su in sorted(repositories_options_dict)],
                        help=(
                            '\n'.join(
                                [su + ''.join(
                                    [' ' for _ in range(total_spaces_to_description - len(su))]) + 'Download from ' +
                                    repositories_options_dict[su]['site_url'] for su in
                                 sorted(repositories_options_dict)])
                        ),
                        required=True)

    return parser.parse_args()


def main():

    with open('repositories.json', 'r') as in_file:
        repo_options_dict = json.load(in_file)

    downloaders_dict = {
        'ccrawl':       CommonCrawlDownloader(repository_name='ccrawl',
                                              site_url=repo_options_dict['ccrawl']['site_url']),

        'dblp':         DBLPDownloader(repository_name='dblp',
                                       site_url=repo_options_dict['dblp']['site_url']),

        'dimacs11':     Dimacs11Downloader(repository_name='dimacs11',
                                           site_url=repo_options_dict['dimacs11']['site_url']),

        'dimacs9':      Dimacs9Downloader(repository_name='dimacs9',
                                          site_url=repo_options_dict['dimacs9']['site_url']),

        'doi':          DOIDownloader(repository_name='doi',
                                      site_url=repo_options_dict['doi']['site_url']),

        'hetrec':       HetrecDownloader(repository_name='hetrec',
                                         site_url=repo_options_dict['hetrec']['site_url']),

        'kone':         KoneDownloader(repository_name='kone',
                                       site_url=repo_options_dict['kone']['site_url']),

        'lalg':         LALGDownloader(repository_name='lalg',
                                       site_url=repo_options_dict['lalg']['site_url']),

        'mvlens':       MVLensDownloader(repository_name='mvlens',
                                         site_url=repo_options_dict['mvlens']['site_url']),

        'nber':         NBERDownloader(repository_name='nber',
                                       site_url=repo_options_dict['nber']['site_url']),

        'netr':         NetworkRepositoryDownloader(repository_name='netr',
                                                    site_url=repo_options_dict['netr']['site_url']),

        'small':        SmallDownloader(repository_name='small',
                                        site_url=repo_options_dict['small']['site_url']),

        'snap':         SNAPDownloader(repository_name='snap',
                                       site_url=repo_options_dict['snap']['site_url']),

        'spmx':         SparseMatrixDownloader(repository_name='spmx',
                                               site_url=repo_options_dict['spmx']['site_url'])
    }

    args = get_args(repo_options_dict)
    output_dir = args.odir

    if not os.path.exists(output_dir):
        try:
            os.mkdir(output_dir)
        except OSError:
            raise

    if args.repo == 'all':
        for downloader_repo in downloaders_dict:
            downloader_output_dir = os.path.join(output_dir, downloader_repo)
            try:
                os.mkdir(downloader_output_dir)
            except FileExistsError:
                pass

            download_logger.info('Downloading {} networks.'.format(downloader_repo))
            downloaders_dict[downloader_repo].download_networks(downloader_output_dir)
    else:
        downloader_repo = args.repo
        downloader_output_dir = os.path.join(output_dir, downloader_repo)

        try:
            os.mkdir(downloader_output_dir)
        except FileExistsError:
            pass

        download_logger.info('Downloading {} networks.'.format(downloader_repo))
        downloaders_dict[downloader_repo].download_networks(downloader_output_dir)


if __name__ == '__main__':
    sys.exit(main())
