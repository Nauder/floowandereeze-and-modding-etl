import logging

from services.data_service import DataService
from services.decode_service import DecodeService
from util import print_splash, BColors, clear_directory, GAME_PATH, NUM_THREADS


def decode_card_data():
    service = DecodeService()
    service.decrypt_desc_indx_name()
    service.decrypt_ids()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s|%(name)s|%(levelname)s]: %(message)s')
    logger = logging.getLogger('main')

    print_splash()
    done_message = BColors.OKCYAN + 'Done' + BColors.ENDC

    logger.info(f'{BColors.OKCYAN}Game path: "{GAME_PATH}"{BColors.ENDC}')
    logger.info(f'{BColors.OKCYAN}Threads to use: {NUM_THREADS}{BColors.ENDC}')

    data_service = DataService()

    logger.info('Getting ids...')
    data_service.get_ids()
    logger.info(done_message)

    logger.info('Decoding card data...')
    decode_card_data()
    logger.info(done_message)

    logger.info('Getting card names...')
    data_service.get_card_names()
    logger.info(done_message)

    # TODO automatically sort fields
    logger.info('Cleaning data...')
    data_service.clean_data()
    logger.info(done_message)

    logger.info('Writing data...')
    data_service.write_data()
    logger.info(done_message)

    logger.info('- Removing temporary files...')
    clear_directory('./etl/services/temp')
    logger.info(done_message)

    logger.info(BColors.OKGREEN + 'ETL Finished' + BColors.ENDC)
