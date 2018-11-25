import optparse
import configparser
from utils import sort_merge_join, CSV_Transformer

from TPCDI_Loader import TPCDI_Loader

if __name__ == "__main__":
    # Parse user's option
    parser = optparse.OptionParser()
    parser.add_option("-s", "--scalefactor", help="Scale factor used")
    parser.add_option(
        "-d", "--dbname", help="Name of database schema to which the data will be loaded")

    (options, args) = parser.parse_args()

    if not (options.scalefactor and options.dbname):
        parser.print_help()
        exit(1)

    # Read and retrieve config from the configfile
    config = configparser.ConfigParser()
    config.read('db.conf')


    # List all available batches in generated flat files
    # batch_numbers = [int(name[5:]) for name in os.listdir('staging/5') if os.path.isdir(os.path.join('staging/5', name))]
    # But le'ts first work on the first batch
    batch_numbers = [1]
    
    for batch_number in batch_numbers:
        # For the historical load, all data are loaded
        if batch_number == 1:
            loader = TPCDI_Loader(options.scalefactor, options.dbname, config, batch_number, overwrite=True)

            # Step 1: Load the batchDate table for this batch
            loader.load_current_batch_date()
        
            # Step 2: Load non-dependence tables
            loader.load_dim_date()
            loader.load_dim_time()
            loader.load_industry()
            loader.load_status_type()
            loader.load_tax_rate()
            loader.load_trade_type()
            loader.load_audit()
            loader.init_di_messages()
            
            # # Step 3: Load staging tables
            loader.load_staging_finwire()
            loader.load_staging_prospect()
            loader.load_staging_broker()
            loader.load_staging_customer()
            loader.load_staging_cash_balances()
            loader.load_staging_watches()
        
            # Step 4: Load dependant table
            loader.load_target_dim_company()
            loader.load_target_financial()
            loader.load_target_dim_security()
            loader.load_prospect()
            loader.load_broker()
    
    
    


    


    