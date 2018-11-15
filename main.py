import optparse
import configparser
from TPCDI_Loader import TPCDI_Loader

if __name__ == "__main__":
  # Parse user's option
  parser = optparse.OptionParser()
  parser.add_option("-s", "--scalefactor", help="Scale factor used")
  parser.add_option("-d", "--dbname", help="Name of database schema to which the data will be loaded")

  (options, args) = parser.parse_args()

  if not (options.scalefactor and options.dbname):
      parser.print_help()
      exit(1)

  # Read and retrieve config from the configfile
  config = configparser.ConfigParser()
  config.read('db.conf')

  # Instantiate TPCDI_Loader and execute the loader in order
  loader = TPCDI_Loader(options.scalefactor, options.dbname, config)
  loader.load_statusType()
  loader.load_taxRate()
  loader.load_tradeType()
  loader.load_audit()