"""Running the Xetra ETL application"""
import logging
import logging.config

import yaml

def main():
    """
        entry point to run the xetra ETL job.
    """
    # Parsing YAML file
    config_path = '/Users/jonmackie/projects/xetra_project/xetra43210-1-2-3-4/configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(config_path))
    
    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")    

if __name__ == '__main__':
    main()