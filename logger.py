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


from functools import wraps
import logging
import time

time_logger = logging.getLogger('TIMER')
time_logger.setLevel(logging.INFO)

download_logger = logging.getLogger('DOWNLOAD')
download_logger.setLevel(logging.INFO)

url_getter_logger = logging.getLogger('URL')
url_getter_logger.setLevel(logging.INFO)


def timer_decorator(log_msg):
    def timing(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            s_time = time.time()
            result = f(*args, **kwargs)
            f_time = time.time()
            logging.getLogger('TIMER').info('It took {:.2f}s to {}.'.format(f_time - s_time, log_msg))
            return result
        return wrapper
    return timing
