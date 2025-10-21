import json
import logging
from datetime import date
from pathlib import Path


def decode_json(json_in: str | Path,
                single: bool = True,
                **kwargs) -> dict | list | None:
    """
    Read a JSON file and return the objects which verify the conditions in input.
    If the input value is None the search will be performed only on the input key.

    :param json_in: The path to the JSON file.
    :type json_in: str | Path
    :param single: Indicates if returns only the first object or the entire list.
    :type single: bool
    :param kwargs: The attribute conditions to be verified as key=value.
    :type kwargs: Any
    :return: A list of matching objects in the file or a single object, or None if no one match.
    :rtype: dict | list | None
    """
    with open(json_in, encoding='utf-8') as jin:
        res = json.load(jin)
    if isinstance(res, dict): res = [res]

    res = [
        obj for obj in res
        # check key and value from input arguments, or just key if value is None
        if all((value is None and key in obj)
               or (obj.get(key) == value)
               for key, value in kwargs.items())
    ]
    # return matching objects as a list or single dictionary
    return res[0] if res and single else res if res else None


def get_logger(fou: str | Path,
               name: str = 'main',
               level: str = 'INFO',
               console: bool = True) -> logging.Logger:
    """
    Initialize a new logger object with custom properties, by creating it if it doesn't already exist.

    :param fou: The path to the log file.
    :type fou: str | Path
    :param name: The name of the logger, defaults to 'main'.
    :type name: str
    :param level: The logging level on file, defaults to INFO.
    :type level: str
    :param console: Enable or disable logging errors also on console, defaults to True.
    :type console: bool
    :return: The logger object.
    :rtype: Logger
    """
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)

        fou = Path(fou).resolve()
        # if input path is a directory will be created a today's log filename
        if fou.is_dir():
            fou = fou / f"{date.today().strftime('%Y_%m_%d')}.log"

        fou_handler = logging.FileHandler(fou)
        fou_handler.setLevel(level)
        fou_handler.setFormatter(logging.Formatter(
            '%(asctime)s.%(msecs)03d [%(name)s] %(levelname)s - %(message)s',
            '%d/%m/%Y %H:%M:%S'
        ))
        logger.addHandler(fou_handler)

        if console:
            # print on console the error or higher log
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.ERROR)
            console_handler.setFormatter(logging.Formatter(
                '%(asctime)s.%(msecs)03d [%(name)s] %(levelname)s - %(message)s',
                '%d/%m/%Y %H:%M:%S'
            ))
            logger.addHandler(console_handler)
    return logger
