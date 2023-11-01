import argparse
import logging
import os
import time

from binancedata.tools.download import file_checksum, file_download, file_unzip
from binancedata.tools.preparation import (
    create_dir,
    get_instru,
    list_from_local,
    list_from_remote,
    list_to_download,
)
from binancedata.tools.utils import dict_from_json

logger = logging.getLogger(__name__)
# TODO: log module_name


class DataDownload:
    json_path = "./config/path.json"
    json_conf = "./config/conf.json"
    json_instru = "./config/instru.json"

    def __init__(
        self,
        is_instru: bool,
        is_remote: bool,
        datatype: str = "candles",
        marketype: str = "spot",
    ) -> None:
        self.is_instru = is_instru
        self.is_remote = is_remote
        self.datatype = datatype
        self.marketype = marketype

        self.path_dic = dict_from_json(self.json_path)
        self.config = dict_from_json(self.json_conf)

        self.config["marketparams"] = self.config["marketparams"][self.marketype]
        self.config["dataparams"] = self.config["dataparams"][self.datatype]

    def prepare(self) -> None:
        path = self.path_dic["data"]

        marketparams = self.config["marketparams"]
        dataparams = self.config["dataparams"]

        datatype = dataparams[0]
        marketype = marketparams[0]
        instru_lst = self.instru_lst

        if os.path.exists(f"{path}/to_download.json"):
            os.remove(f"{path}/to_download.json")

        create_dir(
            path=path,
            instru_lst=instru_lst,
            datatype=datatype,
            marketype=marketype,
        )

        local_lst = list_from_local(
            path=path,
            instru_lst=instru_lst,
            datatype=datatype,
            marketype=marketype,
        )

        remote_lst = list_from_remote(
            path=path,
            is_remote=self.is_remote,
            instru_lst=instru_lst,
            marketparams=marketparams,
            dataparams=dataparams,
        )

        to_download = list_to_download(
            path=path,
            instru_lst=instru_lst,
            local_lst=local_lst,
            remote_lst=remote_lst,
        )
        return to_download

    def download(self, to_down_lst: list):
        datatype = self.config["dataparams"][0]
        marketype = self.config["marketparams"][0]
        path_base = f"{self.path_dic['data']}/{datatype}/raw/{marketype}"
        count_sleep = self.config["count_sleep"]
        count = 0
        for instru in to_down_lst:
            path = f"{path_base}/{instru.replace('/','').lower()}"
            key_lst = to_down_lst[instru]
            for key in key_lst:
                files_down = file_download(path=path, key=key)
                file_checksum(files=files_down)
                file_unzip(file=files_down[0])
                count += 1
                if count >= count_sleep:
                    count = 0
                    time.sleep(1)

    def start(self):
        banned_instru = self.config["banned_instru"]
        self.instru_lst = get_instru(
            file=self.json_instru,
            is_instru=self.is_instru,
            banned_instru=banned_instru,
            num_instru=self.config["num_instru"],
            marketype=self.marketype,
            marketparams=self.config["marketparams"],
        )

        to_download = self.prepare()

        self.download(to_down_lst=to_download)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download arguments")
    parser.add_argument(
        "-i",
        "--instru",
        type=str,
        choices=["true", "false"],
        default="false",
        help="if fetach instru list from remote",
    )

    parser.add_argument(
        "-r",
        "--remote",
        type=str,
        choices=["true", "false"],
        default="true",
        help="if update local remote list",
    )

    parser.add_argument(
        "-m",
        "--marketype",
        type=str,
        default="spot",
        help="specify market type",
    )

    parser.add_argument(
        "-d",
        "--datatype",
        type=str,
        default="candles",
        help="specify data type",
    )

    args = parser.parse_args()
    is_instru = True if args.instru == "true" else False
    is_remote = True if args.remote == "true" else False
    marketype = args.marketype
    datatype = args.datatype

    dwnldr = DataDownload(
        is_instru=is_instru, is_remote=is_remote, datatype=datatype, marketype=marketype
    )
    dwnldr.start()
