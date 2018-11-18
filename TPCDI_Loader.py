import os
import glob

from utils import prepare_char_insertion, prepare_numeric_insertion

class TPCDI_Loader():
  BASE_MYSQL_CMD = ""

  def __init__(self, sf, db_name, config):
    """
    Initialize target database.

    Attributes:
        sf (str): Scale factor to be used in benchmark.
        db_name (str): Name of database schema to which the data will be loaded.    
        config (config list): Config object retrieved from calling ConfigParser().read().
    """

    self.sf = sf
    self.db_name = db_name

    # Construct base mysql command (set host, port, and user)
    TPCDI_Loader.BASE_MYSQL_CMD = "mysql -h "+config['MEMSQL_SERVER']['memsql_host']+" -u "+config['MEMSQL_SERVER']['memsql_user']+" -P "+config['MEMSQL_SERVER']['memsql_port']
    
    # Create initial database
    # TODO: Check if database exists
    db_creation_ddl = "CREATE DATABASE "+self.db_name
    
    # Construct MySQL client bash command and then execute it
    db_creation_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -e '"+db_creation_ddl+"'"
    os.system(db_creation_cmd)

  def load_dimDate(self):
    """
    Create DimDate table in the target database and then load rows in Date.txt into it.
    """

    # Create ddl to store dimDate
    dimDate_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE DimDate (
      SK_DateID INTEGER NOT NULL PRIMARY KEY,
			DateValue DATE NOT NULL,
			DateDesc CHAR(20) NOT NULL,
			CalendarYearID NUMERIC(4) NOT NULL,
			CalendarYearDesc CHAR(20) NOT NULL,
			CalendarQtrID NUMERIC(5) NOT NULL,
			CalendarQtrDesc CHAR(20) NOT NULL,
			CalendarMonthID NUMERIC(6) NOT NULL,
			CalendarMonthDesc CHAR(20) NOT NULL,
			CalendarWeekID NUMERIC(6) NOT NULL,
			CalendarWeekDesc CHAR(20) NOT NULL,
			DayOfWeeknumeric NUMERIC(1) NOT NULL,
			DayOfWeekDesc CHAR(10) NOT NULL,
			FiscalYearID NUMERIC(4) NOT NULL,
			FiscalYearDesc CHAR(20) NOT NULL,
			FiscalQtrID NUMERIC(5) NOT NULL,
			FiscalQtrDesc CHAR(20) NOT NULL,
			HolidayFlag BOOLEAN
    );
    """

    # Create query to load text data into dimDate table
    dimDate_load_query="LOAD DATA LOCAL INFILE 'staging/"+self.sf+"/Batch1/Date.txt' INTO TABLE DimDate COLUMNS TERMINATED BY '|';"
    
    # Construct mysql client bash command to execute ddl and data loading query
    dimDate_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+dimDate_ddl+"\""
    dimDate_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+dimDate_load_query+"\""
    
    # Execute the command
    os.system(dimDate_ddl_cmd)
    os.system(dimDate_load_cmd)

  def init_diMessages(self):
    """
    Create DImessages table in the target database.
    """

    # Create ddl to store dimTime
    diMessages_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE DImessages (
      MessageDateAndTime TIMESTAMP NOT NULL,
			BatchID NUMERIC(5) NOT NULL,
			MessageSource CHAR(30),
			MessageText CHAR(50) NOT NULL,
			MessageType CHAR(12) NOT NULL,
			MessageData CHAR(100)
    );
    """

    diMessages_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+diMessages_ddl+"\""
    os.system(diMessages_ddl_cmd)

  def load_dimTime(self):
    """
    Create DimTime table in the target database and then load rows in Time.txt into it.
    """

    # Create ddl to store dimTime
    dimTime_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE DimTime (
      SK_TimeID INTEGER Not NULL PRIMARY KEY,
			TimeValue TIME Not NULL,
			HourID numeric(2) Not NULL,
			HourDesc CHAR(20) Not NULL,
			MinuteID numeric(2) Not NULL,
			MinuteDesc CHAR(20) Not NULL,
			SecondID numeric(2) Not NULL,
			SecondDesc CHAR(20) Not NULL,
			MarketHoursFlag BOOLEAN,
			OfficeHoursFlag BOOLEAN
    );
    """

    # Create query to load text data into dimTime table
    dimTime_load_query="LOAD DATA LOCAL INFILE 'staging/"+self.sf+"/Batch1/Time.txt' INTO TABLE DimTime COLUMNS TERMINATED BY '|';"
    
    # Construct mysql client bash command to execute ddl and data loading query
    dimTime_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+dimTime_ddl+"\""
    dimTime_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+dimTime_load_query+"\""
    
    # Execute the command
    os.system(dimTime_ddl_cmd)
    os.system(dimTime_load_cmd)

  def load_Industry(self):
    """
    Create Industry table in the target database and then load rows in Industry.txt into it.
    """

    # Create ddl to store industry
    industry_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE Industry (
      IN_ID CHAR(2) Not NULL,
			IN_NAME CHAR(50) Not NULL,
			IN_SC_ID CHAR(4) Not NULL
    );
    """

    # Create query to load text data into industry table
    industry_load_query="LOAD DATA LOCAL INFILE 'staging/"+self.sf+"/Batch1/Industry.txt' INTO TABLE Industry COLUMNS TERMINATED BY '|';"
    
    # Construct mysql client bash command to execute ddl and data loading query
    industry_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+industry_ddl+"\""
    industry_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+industry_load_query+"\""
    
    # Execute the command
    os.system(industry_ddl_cmd)
    os.system(industry_load_cmd)

  def load_statusType(self):
    """
    Create StatusType table in the target database and then load rows in StatusType.txt into it.
    """

      
    # Create ddl to store statusType
    statusType_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE StatusType (
      ST_ID CHAR(4) NOT NULL,
      ST_NAME CHAR(10) NOT NULL
    );
    """

    # Create query to load text data into statusType table
    statusType_load_query="LOAD DATA LOCAL INFILE 'staging/"+self.sf+"/Batch1/StatusType.txt' INTO TABLE StatusType COLUMNS TERMINATED BY '|';"
    
    # Construct mysql client bash command to execute ddl and data loading query
    statusType_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+statusType_ddl+"\""
    statusType_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+statusType_load_query+"\""
    
    # Execute the command
    os.system(statusType_ddl_cmd)
    os.system(statusType_load_cmd)

  def load_taxRate(self):
    """
    Create TaxRate table in the target database and then load rows in TaxRate.txt into it.
    """

    # Create ddl to store taxRate
    taxRate_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE TaxRate (
      TX_ID CHAR(4) NOT NULL,
      TX_NAME CHAR(50) NOT NULL,
			TX_RATE NUMERIC(6,5) NOT NULL
    );
    """

    # Create query to load text data into taxRate table
    taxRate_load_query="LOAD DATA LOCAL INFILE 'staging/"+self.sf+"/Batch1/TaxRate.txt' INTO TABLE TaxRate COLUMNS TERMINATED BY '|';"
    
    # Construct mysql client bash command to execute ddl and data loading query
    taxRate_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+taxRate_ddl+"\""
    taxRate_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+taxRate_load_query+"\""
    
    # Execute the command
    os.system(taxRate_ddl_cmd)
    os.system(taxRate_load_cmd)
  
  def load_tradeType(self):
    """
    Create TradeType table in the target database and then load rows in TradeType.txt into it.
    """

    # Create ddl to store tradeType
    tradeType_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE TradeType (
      TT_ID CHAR(3) NOT NULL,
      TT_NAME CHAR(12) NOT NULL,
			TT_IS_SELL NUMERIC(1) NOT NULL,
			TT_IS_MRKT NUMERIC(1) NOT NULL
    );
    """

    # Create query to load text data into tradeType table
    tradeType_load_query="LOAD DATA LOCAL INFILE 'staging/"+self.sf+"/Batch1/TradeType.txt' INTO TABLE TradeType COLUMNS TERMINATED BY '|';"
    
    # Construct mysql client bash command to execute ddl and data loading query
    tradeType_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+tradeType_ddl+"\""
    tradeType_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+tradeType_load_query+"\""
    
    # Execute the command
    os.system(tradeType_ddl_cmd)
    os.system(tradeType_load_cmd)

  def load_prospect(self):
    """
    Create Prospect table in the target database and then load rows in Prospect.csv into it.
    """

    # Create ddl to store prospect
    prospect_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE Prospect (
      AgencyID CHAR(30) NOT NULL,  
			SK_RecordDateID INTEGER NOT NULL,
      SK_UpdateDateID INTEGER NOT NULL,
			BatchID NUMERIC(5) NOT NULL,
			IsCustomer BOOLEAN NOT NULL,
			LastName CHAR(30) NOT NULL,
			FirstName CHAR(30) NOT NULL,
			MiddleInitial CHAR(1),
			Gender CHAR(1),
			AddressLine1 CHAR(80),
			AddressLine2 CHAR(80),
			PostalCode CHAR(12),
			City CHAR(25) NOT NULL,
			State CHAR(20) NOT NULL,
			Country CHAR(24),
			Phone CHAR(30), 
			Income NUMERIC(9),
			NumberCars NUMERIC(2), 
			NumberChildren NUMERIC(2), 
			MaritalStatus CHAR(1), 
			Age NUMERIC(3),
			CreditRating NUMERIC(4),
			OwnOrRentFlag CHAR(1), 
			Employer CHAR(30),
			NumberCreditCards NUMERIC(2), 
			NetWorth NUMERIC(12),
			MarketingNameplate CHAR(100)
    );
    """

    # Create query to load text data into prospect table
    prospect_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+prospect_ddl+"\""
    # Execute the command
    os.system(prospect_ddl_cmd)

    batch_date = ''
    with open("staging/"+self.sf+"/Batch1/BatchDate.txt") as f:
	    batch_date = f.readline()

    with open("staging/"+self.sf+"/Batch1/Prospect.csv") as prospects:
      inserted_rows = 0
      for prospect in prospects:
        agency_id, last_name, first_name, middle_initial, gender, address_line_1, address_line_2, postal_code, city, state, country, \
        phone, income, number_cars, number_children, marital_status, age, credit_rating, own_or_rent_flag, employer, number_credit_cards, \
        net_worth = prospect.split(",")
        marketing_nameplate = ''
        is_customer = "TRUE"
        
        try:  
          if int(net_worth) > 1000000 or int(income) > 200000:
            marketing_nameplate += '+HighValue'
        except ValueError:
          pass
        try:
          if int(number_children) > 3 or int(number_credit_cards) > 5:
            marketing_nameplate += '+Expenses'
        except ValueError:
          pass
        try:
          if int(age) > 45:
            marketing_nameplate += '+Boomer'
        except ValueError:
          pass
        try:
          if int(income) < 5000 or int(credit_rating) < 600 or int(net_worth) < 100000:
            marketing_nameplate += '+MoneyAlert'
        except ValueError:
          pass
        try:
          if int(number_cars) > 3 or int(number_credit_cards) > 7:
            marketing_nameplate += '+Spender'
        except ValueError:
          pass
        try:
          if int(age) < 25 or int(net_worth) > 1000000:
            marketing_nameplate += '+Inherited'
        except ValueError:
          pass
          
        if marketing_nameplate:
          marketing_nameplate = prepare_char_insertion(marketing_nameplate[1:]) # remove the additionaly '+' the excessive '+' character in the first index of string 
        else:
          marketing_nameplate = "''"
        batch_id = 1

        agency_id = prepare_char_insertion(agency_id)
        last_name = prepare_char_insertion(last_name)
        first_name = prepare_char_insertion(first_name)
        middle_initial = prepare_char_insertion(middle_initial)
        gender = prepare_char_insertion(gender)
        address_line_1 = prepare_char_insertion(address_line_1)
        address_line_2 = prepare_char_insertion(address_line_2)
        postal_code = prepare_char_insertion(postal_code)
        city = prepare_char_insertion(city)
        state = prepare_char_insertion(state)
        country = prepare_char_insertion(country)
        phone = prepare_char_insertion(phone)
        marital_status = prepare_char_insertion(marital_status)
        own_or_rent_flag = prepare_char_insertion(own_or_rent_flag)
        employer = prepare_char_insertion(employer)

        income = prepare_numeric_insertion(income)
        number_cars = prepare_numeric_insertion(number_cars)
        number_children = prepare_numeric_insertion(number_children)
        age = prepare_numeric_insertion(age)
        credit_rating = prepare_numeric_insertion(credit_rating)
        number_credit_cards = prepare_numeric_insertion(number_credit_cards)
        net_worth = prepare_numeric_insertion(net_worth)

        # Construct mysql client bash command to execute data insertion query
        prospect_load_query = f"INSERT INTO Prospect VALUES ({agency_id}, {batch_date}, {batch_date}, {batch_id}, {is_customer}, {last_name}, {first_name}, {middle_initial}, {gender}, {address_line_1}, {address_line_2}, {postal_code}, {city}, {state}, {country}, {phone}, {income}, {number_cars}, {number_children}, {marital_status}, {age}, {credit_rating}, {own_or_rent_flag}, {employer}, {number_credit_cards}, {net_worth}, {marketing_nameplate})"
        prospect_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+prospect_load_query+"\""
        # Execute the command
        os.system(prospect_load_cmd)
        inserted_rows += 1

      message_insert_query = f"INSERT INTO DImessages VALUES(CURRENT_TIMESTAMP(), {batch_id}, 'Prospect', 'Inserted rows', 'Status', '{str(inserted_rows)}')"
      message_insert_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+message_insert_query+"\""
      os.system(message_insert_cmd)

  def load_audit(self):
    """
    Create Audit table in the target database and then load rows in files with "_audit.csv" ending into it.
    """

    # Create ddl to store audit
    audit_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE Audit (
      DataSet CHAR(20) NOT Null,
			BatchID NUMERIC(5),
			AT_Date DATE,
			AT_Attribute CHAR(50),
			AT_Value NUMERIC(15),
			DValue NUMERIC(15,5)
    );
    """

    audit_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+audit_ddl+"\""
    os.system(audit_ddl_cmd)

    for filepath in glob.iglob("staging/"+self.sf+"/Batch1/*_audit.csv"):# Create query to load text data into tradeType table
      audit_load_query="LOAD DATA LOCAL INFILE '"+filepath+"' INTO TABLE Audit COLUMNS TERMINATED BY ',' IGNORE 1 LINES;"
      
      # Construct mysql client bash command to execute ddl and data loading query
      audit_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+audit_load_query+"\""
      
      # Execute the command
      os.system(audit_load_cmd)
