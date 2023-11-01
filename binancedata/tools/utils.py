import json
import logging

logger = logging.getLogger(__name__)


def dict_from_json(file):
    result = {}
    try:
        result = json.load(open(file, "r"))
    except Exception as e:
        content = f"config load error: {e}"
        print(content)
        logger.error(content)

    return result
