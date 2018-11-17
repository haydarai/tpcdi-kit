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

    # Instantiate TPCDI_Loader and execute the loader in order
    # First, load all flat files to staging tables
    loader = TPCDI_Loader(options.scalefactor, options.dbname, config)
    loader.load_staging_dimDate()
    loader.load_staging_dimTime()
    loader.load_staging_Industry()
    loader.load_staging_statusType()
    loader.load_staging_taxRate()
    loader.load_staging_tradeType()
    loader.load_audit()
    loader.init_diMessages()
    loader.load_prospect()
    loader.init_dimCustomer()
    loader.load_staging_company()
    
    loader.load_target_dim_company()
