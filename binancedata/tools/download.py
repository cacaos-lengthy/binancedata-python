import logging
import os
import subprocess
import sys
import urllib

import retry
from config import URL_Base_Download

logger = logging.getLogger(__name__)


def file_download(path: str, key: str):
    @retry.retry(tries=3, jitter=5)
    def download_one(url: str, path: str):
        file = f"{path}/{url.split('/')[-1]}"
        try:
            dl_file = urllib.request.urlopen(url)
            length = dl_file.getheader("content-length")
            if length:
                length = int(length)
                blocksize = max(4096, length // 100)

            with open(file, "wb") as out_file:
                dl_progress = 0
                logger.info(f"file download: {file}")
                while True:
                    buf = dl_file.read(blocksize)
                    if not buf:
                        break
                    dl_progress += len(buf)
                    out_file.write(buf)
                    done = int(50 * dl_progress / length)
                    sys.stdout.write("\r[%s%s]" % ("#" * done, "." * (50 - done)))
                    sys.stdout.flush()

        except urllib.error.HTTPError:
            msg = f"file not found: {url}"
            logger.error(msg=msg)
        return file

    url_lst = [
        f"{URL_Base_Download}/{key}",
        f"{URL_Base_Download}/{key}.CHECKSUM",
    ]
    saved_files = []
    for i in url_lst:
        file_download = download_one(url=i, path=path)
        saved_files.append(file_download)
    return saved_files


def file_checksum(files):
    path = os.path.dirname(files[0])
    file_check = os.path.basename(files[-1])

    result = subprocess.check_output(
        f"cd {path} && shasum -a 256 -c {file_check}", shell=True
    )
    result = result.decode("utf-8").lower()
    os.remove(files[-1])
    if "ok" not in result:
        os.remove(files[0])


def file_unzip(file):
    path = os.path.dirname(file)
    filename = os.path.basename(file)
    result = subprocess.check_output(f"cd {path} && tar xvf {filename}", shell=True)
    result = result.decode("utf-8").lower()
    os.remove(file)
