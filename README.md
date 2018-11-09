# TPC-DI Kit

## Usage
First, generate TPC-DI data using `Tools/DIGen.jar` and store it in `staging/<scalefactor>/` directory. For example, using scalefactor of 5 you can run:

    $ mkdir staging/5/
    $ cd Tools/
    $ java -jar DIGen.jar -o ../staging/5/ -sf 5

### Options:

    Usage: main.py [options]
    
    Options:
      -h, --help            show this help message and exit
      -s SCALEFACTOR, --scalefactor=SCALEFACTOR
                            Scale factor used
      -d DBNAME, --dbname=DBNAME
                            Name of database schema to which the data will be
                            loaded

### Example:

    $ python main.py -d tpcdi5 -sf 5
