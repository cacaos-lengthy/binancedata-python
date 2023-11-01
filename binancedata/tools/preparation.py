import json
import logging
import os
import urllib.request

import requests
import retry
import xmltodict
from config import URL_Base_S3, URL_Ticker_CFuture, Url_Ticker_Spot, URL_Ticker_UFuture

from binancedata.tools.utils import dict_from_json

logger = logging.getLogger(__name__)


def get_instru(
    file: str = "./instru.json",
    is_instru: bool = False,
    num_instru: int = 1,
    banned_instru: list = [],
    marketype: str = "spot",
    marketparams: list = ["futures", "um"],
    **kwargs,
):
    @retry.retry(tries=3, jitter=5)
    def instru_by_value(marketparams):
        url = None
        if marketype == "spot":
            url = Url_Ticker_Spot
        elif marketype == "ufuture":
            url = URL_Ticker_UFuture
        elif marketype == "cfuture":
            url = URL_Ticker_CFuture

        response = urllib.request.urlopen(url).read()
        response = json.loads(response)
        info_lst = [(i["symbol"], float(i["quoteVolume"])) for i in response]
        info_lst = [i for i in info_lst if i[0].upper().endswith("USDT")]
        info_lst.sort(key=lambda i: i[1], reverse=True)
        info_lst = [(i[0].lower().replace("usdt", "/usdt"), i[1]) for i in info_lst]
        instru_lst = [i[0] for i in info_lst]
        instru_lst = [i for i in instru_lst if i not in banned_instru]
        return instru_lst

    instru_lst_local = dict_from_json(file=file)
    if is_instru:  # fetch new list from remote
        instru_lst = instru_by_value(marketparams=marketparams)[:num_instru]
        instru_dic = {marketype: instru_lst}
        # update local
        instru_lst_local.update(instru_dic)
        instru_json = json.dumps(instru_lst_local, indent=4)
        with open(file, "w") as file:
            file.write(instru_json)
    else:
        instru_lst = instru_lst_local[marketype][:num_instru]

    return instru_lst


def create_dir(
    path: str = "./",
    instru_lst: list = [],
    datatype: str = "klines",
    marketype: str = "spot",
    **kwargs,
):
    """create directory"""
    if not os.path.exists(path):
        os.makedirs(path)
    for i in instru_lst:
        dir_instru = f"{path}/{datatype}/raw/{marketype}/{i.replace('/','').lower()}"
        if not os.path.exists(dir_instru):
            os.makedirs(dir_instru)


def list_from_local(
    instru_lst: str = [],
    datatype: str = "klines",
    marketype: str = "spot",
    path: str = "./",
    **kwargs,
):
    """
    get local files list
    """
    dates_dic = {}
    for i in instru_lst:
        dir_instru = f"{path}/{datatype}/raw/{marketype}/{i.replace('/','').lower()}"
        try:
            files = os.listdir(dir_instru)
            files = [i for i in files if i.endswith(".csv")]
            dates = ["-".join(i.split(".csv")[0].split("-")[-3:]) for i in files]
            dates_dic[i] = dates

            files_rm = [i for i in files if not i.endswith(".csv")]
            for file in files_rm:
                os.remove(file)
        except Exception as e:
            logger.error(f"list from local: {str(e)}")

    return dates_dic


def list_from_remote(
    path: str = ".",
    is_remote: bool = False,
    instru_lst: list = [],
    marketparams: list = ["futures", "um"],
    dataparams: list = ["klines", "1m"],
    **kwargs,
):
    """
    get files list from remote: S3 server
    """

    @retry.retry(tries=3, jitter=5)
    def _get_keys_from_s3(url):
        """fetch from s3"""
        key_lst = []
        is_truncated = True
        params = None
        while is_truncated:
            result = requests.get(url, params=params).content
            result = xmltodict.parse(result)["ListBucketResult"]
            key_lst += result["Contents"]

            is_truncated = True if result["IsTruncated"] == "true" else False
            if is_truncated:
                params = {"marker": result["NextMarker"]}

        # rm checksum
        key_lst = [i["Key"] for i in key_lst if i["Key"].endswith("zip")]

        date_dic = {
            "-".join(i.split("/")[-1].split(".")[0].split("-")[-3:]): i for i in key_lst
        }
        return date_dic

    file = f"{path}/remote.json"
    key_dic = {}
    if (not is_remote) and os.path.exists(file):
        key_dic = json.load(open(file, "r"))
        count = sum([len(key_dic[i]) for i in key_dic])
        if count <= 0:
            os.remove(file)
    else:
        intvl = "daily"
        url_mkt = "/".join(marketparams)
        url_instru_dic = {
            i: "/".join(dataparams[:1] + [i.replace("/", "").upper()] + dataparams[1:])
            for i in instru_lst
        }
        # prepare for requests
        url_dic = {
            i: f"{URL_Base_S3}?delimiter=/&prefix=data/{url_mkt}/{intvl}/{url_instru_dic[i]}/"
            for i in url_instru_dic
        }

        for instru in url_dic:
            url = url_dic[instru]
            try:
                key_dic[instru] = _get_keys_from_s3(url=url)
            except Exception as e:
                logger.error(msg=f"remote keys error: {url}")
                logger.error(msg=f"error: {str(e)}")

        key_json = json.dumps(key_dic, indent=4)
        with open(file, "w") as f:
            f.write(key_json)

    return key_dic


def list_to_download(
    path: str = "./",
    instru_lst: list = [],
    local_lst: list = [],
    remote_lst: list = [],
    **kwargs,
):
    """
    get to-download list
    """

    download_dic = {}
    error_info = []
    for instru in instru_lst:
        if instru not in remote_lst:
            msg = f"missing {instru} from remote."
            error_info.append(msg)
            logger.error(msg=msg)
            continue

        if instru not in local_lst:
            download_dic[instru] = [remote_lst[instru][i] for i in remote_lst[instru]]
            logger.info("all")

        tmp = [
            remote_lst[instru][i]
            for i in remote_lst[instru]
            if i not in local_lst[instru]
        ]
        download_dic[instru] = tmp

    download_josn = json.dumps(download_dic, indent=4)
    with open(f"{path}/to_download.json", "w") as file:
        file.write(download_josn)

    logger.info(msg=f"error info: {error_info}")
    return download_dic
