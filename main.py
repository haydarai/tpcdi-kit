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
    loader = TPCDI_Loader(options.scalefactor, options.dbname, config)
    loader.load_dimDate()
    loader.load_dimTime()
    loader.load_Industry()
    loader.load_statusType()
    loader.load_taxRate()
    loader.load_tradeType()
    loader.load_audit()
    loader.init_diMessages()
    loader.load_prospect()

    # Stupid example, joining the same Industry csv file
    # It will return iterator
    pipe_delimited_transformer = CSV_Transformer(delimiter="|")
    res = sort_merge_join('staging/5/Batch1/Industry.txt', 'staging/5/Batch1/Industry.txt',
                        0, 0, pipe_delimited_transformer, pipe_delimited_transformer)

    # Use next() to get the next result
    print(next(res))
    print(next(res))
