#!/usr/bin/env python

import os 
import time
import glob
import logging
import datetime
import ConfigParser
from pymongo import MongoClient, UpdateOne, errors

config = ConfigParser.RawConfigParser()
config.read("config.ini")


try:
    # get logging configs
    logger = logging.getLogger()
    logging.basicConfig(filename=config.get('logging', 'log_file'), format=config.get('logging', 'format'))

    if config.get('logging', 'disabled') == "1":
        logger.disabled = True
    
    level = logging.getLevelName(config.get('logging', 'level'))
    logger.setLevel(level)
    logging.info('Logging configuration loaded from config file.')

except ConfigParser.NoOptionError, e:
    # set default logging params
    logging.basicConfig(filename='log/debug.log', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    logging.info('Using default logging configuration')

logging.info('Service Started!')

try:
    con_string = 'mongodb://{0}:{1}@{2}:{3}/{4}'.format(
                                config.get('mongo', 'user'), 
                                config.get('mongo', 'password'), 
                                config.get('mongo', 'host'), 
                                config.get('mongo', 'port'), 
                                config.get('mongo', 'database')
                                )

    client = MongoClient(con_string)
    db = client[config.get('mongo', 'database')]
    external_users = db.externalusers

    # company configurations
    company_id = int(config.get('organization', 'company'))
    tenant_id = int(config.get('organization', 'tenant'))
except Exception, e:
    logging.exception(e)
    raise


# start the main process.
def main():
    try:
        os.chdir(config.get('data', 'data_dir'))

        #set processed data dir path.
        processed_dir = config.get('data', 'processed_dir')

        while True:
            
            for file in sorted(glob.glob("*.txt"), key=os.path.getctime):
                with open(file) as fp:
                    operations = []
                    line = fp.readline()
                    logging.info("Recieved file: " + file)

                    while line:
                        line = line.strip()
                        customer_data = line.split('|')
                        
                        if len(customer_data) == 4:
                            customer_name = customer_data[0].split(',')
                            operations.append(
                                UpdateOne(
                                    { "ssn": customer_data[1] },
                                    { 
                                        "$setOnInsert": { "created_at": datetime.datetime.utcnow() },
                                        "$currentDate": { "updated_at": True },
                                        "$set": 
                                        { 
                                            "firstname": customer_name[1].strip() if len(customer_name) > 1 else '',
                                            "lastname": customer_name[0].strip(),
                                            "phone": customer_data[2],
                                            "company" : company_id,
                                            "tenant" : tenant_id,
                                            "thirdpartyreference": customer_data[1]
                                        },
                                    }
                                , upsert=True)
                            )

                        line = fp.readline()

                    if len(operations) > 0:    
                        logging.info("Executing batch upsert!")

                        try:
                            result = external_users.bulk_write(operations, ordered=False)

                            # Upserted IDs are not required to log.
                            # logging.debug("Upserted IDs: " + str(result.upserted_ids))

                            logging.info("Result: Total Records found = {}, Matched = {}, Inserted = {}, Modified = {}, Upserted = {}".format(
                                len(operations), result.matched_count, result.inserted_count, result.modified_count, result.upserted_count
                            ))
                            
                        except errors.BulkWriteError as be:
                            logging.debug("Bulk operation completed with some errors:")
                            logging.info(be.details)
                        
                    else:
                        logging.info("No records to update.")

                     # move the current file to the processed directory
                    logging.info("Moving {} to {}".format(file, processed_dir + "/" + file + "._" + str(time.time())))
                    os.rename(file, processed_dir + "/" + file + "._" + str(time.time()))

            time.sleep(10)

    except Exception as e:
        logging.exception(e)
        raise


if __name__ == "__main__":
    main()