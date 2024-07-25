from helper.logger_helper import LoggerSimple
import time
import requests
import sys
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor


logger = LoggerSimple.get_logger(__name__)


def multithread_helper(items, method, max_workers=2, timeout_concurrent_by_second=260, debug=True):
    output = []
    start = time.time()
    logger.info(f'Start time: {start}')
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {executor.submit(
            method, item): item for item in items}
        logger.info(f'Futures submitted {len(future_to_item)}')
        logger.debug(future_to_item)
        logger.info(f'Loading multithread for method {str(method)}')
        completed_futures = 0
        total_futures = 0
        total_futures = len(future_to_item)
        for future in concurrent.futures.as_completed(future_to_item, timeout=timeout_concurrent_by_second):
            item = future_to_item[future]
            try:
                data = future.result()
                if data is not None and data != '':
                    output.append(data)
                completed_futures += 1
            except Exception as e:
                logger.error(f'Exception for item {item} {e}')
                logger.error(f'Error on line {\
                             sys.exc_info()[-1].tb_lineno} {e}')
            else:
                if debug:
                    logger.info('"%s" fetched in %ss' %
                                (item, (time.time() - start)))
        if debug:
            elapsed_time = time.time() - start
            logger.debug(f'Elapsed Time {elapsed_time}')
            logger.info(f'Completed futures: {\
                        completed_futures} of {total_futures}')
            logger.debug(f'Output: {output}')
        return output
