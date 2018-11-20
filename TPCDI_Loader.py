import os
import glob

from utils import prepare_char_insertion, prepare_numeric_insertion

class TPCDI_Loader():
  BASE_MYSQL_CMD = ""

  def __init__(self, sf, db_name, config, batch_number, overwrite=False):
    """
    Initialize staging database.

    Attributes:
        sf (str): Scale factor to be used in benchmark.
        db_name (str): Name of database schema to which the data will be loaded.    
        config (config list): Config object retrieved from calling ConfigParser().read().
        batch_number (int): Batch number that going to be processed
    """

    self.sf = sf
    self.db_name = db_name
    self.batch_number = batch_number
    self.batch_dir = "staging/"+self.sf+"/Batch"+str(self.batch_number)+"/"

    # Construct base mysql command (set host, port, and user)
    TPCDI_Loader.BASE_MYSQL_CMD = "mysql -h "+config['MEMSQL_SERVER']['memsql_host']+" -u "+config['MEMSQL_SERVER']['memsql_user']+" -P "+config['MEMSQL_SERVER']['memsql_port']
    
    # Drop database if it is exist and overwrite param is set to True
    if overwrite:
      drop_db_query = "DROP DATABASE IF EXISTS "+self.db_name
      drop_db_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -e '"+drop_db_query+"'"
      os.system(drop_db_cmd)
    
    db_creation_ddl = "CREATE DATABASE "+self.db_name
    
    # Construct MySQL client bash command and then execute it
    db_creation_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -e '"+db_creation_ddl+"'"
    os.system(db_creation_cmd)

    # Insert create batch date table
    batch_date_ddl = "CREATE TABLE batch_date(batch_number NUMERIC(3), batch_date DATE);"
    batch_date_creation_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+batch_date_ddl+"\""
    os.system(batch_date_creation_cmd)

  def load_current_batch_date(self):
    with open(self.batch_dir+"BatchDate.txt", "r") as batch_date_file:
      batch_date_loading_query = "INSERT INTO batch_date VALUES (%i, STR_TO_DATE('%s','%s'));"%(self.batch_number,batch_date_file.read().strip(),"%Y-%m-%d")
      batch_date_loading_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+batch_date_loading_query+"\""      
      os.system(batch_date_loading_cmd)
  
  def load_dim_date(self):
    """
    Create DimDate table in the staging database and then load rows in Date.txt into it.
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

  def init_di_messages(self):
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

  def init_dim_customer(self):
    """
    Create DimCustomer table in the target database.
    """

    # Create ddl to store dimCustomer
    dimCustomer_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE DimCustomer (
      SK_CustomerID INTEGER NOT NULL PRIMARY KEY,
			CustomerID INTEGER NOT NULL,
			TaxID CHAR(20) NOT NULL,
			Status CHAR(10) NOT NULL,
			LastName CHAR(30) NOT NULL,
			FirstName CHAR(30) NOT NULL,
			MiddleInitial CHAR(1),
			Gender CHAR(1),
			Tier INTEGER,
			DOB date NOT NULL,
			AddressLine1 VARCHAR(80) NOT NULL,
			AddressLine2 VARCHAR(80),
			PostalCode CHAR(12) NOT NULL,
			City CHAR(25) NOT NULL,
			Country	CHAR(24),
			StateProv	CHAR(20) NOT NULL,
			Phone1 CHAR(30),
			Phone2 CHAR(30),
			Phone3 CHAR(30),
			Email1 CHAR(50),
			Email2 CHAR(50),
			NationalTaxRateDesc	VARCHAR(50),
			NationalTaxRate	NUMERIC(6,5),
			LocalTaxRateDesc VARCHAR(50),
			LocalTaxRate NUMERIC(6,5),
			AgencyID CHAR(30),
			CreditRating INTEGER,
			NetWorth NUMERIC(10),
			MarketingNameplate VARCHAR(100),
			IsCurrent BOOLEAN NOT NULL,
			BatchID INTEGER NOT NULL,
			EffectiveDate DATE NOT NULL,
			EndDate DATE NOT NULL
    );
    """

    dimCustomer_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+dimCustomer_ddl+"\""
    os.system(dimCustomer_ddl_cmd)

  def load_dim_time(self):
    """
    Create DimTime table in the staging database and then load rows in Time.txt into it.
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

  def load_industry(self):
    """
    Create Industry table in the staging database and then load rows in Industry.txt into it.
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

  def load_status_type(self):
    """
    Create StatusType table in the staging database and then load rows in StatusType.txt into it.
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

  def load_tax_rate(self):
    """
    Create TaxRate table in the staging database and then load rows in TaxRate.txt into it.
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
  
  def load_trade_type(self):
    """
    Create TradeType table in the staging database and then load rows in TradeType.txt into it.
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

  def load_staging_prospect(self):
    """
    Create S_Prospect table in the staging database and then load rows in Prospect.csv into it.
    """

    # Create ddl to store prospect
    prospect_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE S_Prospect(
      AGENCY_ID CHAR(30) NOT NULL,
      LAST_NAME CHAR(30) NOT NULL,
      FIRST_NAME CHAR(30) NOT NULL,
      MIDDLE_INITIAL CHAR(1),
      GENDER CHAR(1),
      ADDRESS_LINE_1 CHAR(80),
      ADDRESS_LINE_2 CHAR(80),
      POSTAL_CODE CHAR(12),
      CITY CHAR(25) NOT NULL,
      STATE CHAR(20) NOT NULL,
      COUNTRY CHAR(24),
      PHONE CHAR(30),
      INCOME NUMERIC(9),
      NUMBER_CARS NUMERIC(2),
      NUMBER_CHILDREM NUMERIC(2),
      MARITAL_STATUS CHAR(1),
      AGE NUMERIC(3),
      CREDIT_RATING NUMERIC(4),
      OWN_OR_RENT_FLAG CHAR(1),
      EMPLOYER CHAR(30),
      NUMBER_CREDIT_CARDS NUMERIC(2),
      NET_WORTH NUMERIC(12)
    );
    """

    # Create query to load text data into prospect table
    prospect_load_query="LOAD DATA LOCAL INFILE 'staging/"+self.sf+"/Batch1/Prospect.CSV' INTO TABLE S_Prospect COLUMNS TERMINATED BY ',';"
    
    # Construct mysql client bash command to execute ddl and data loading query
    prospect_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+prospect_ddl+"\""
    prospect_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+prospect_load_query+"\""
    
    # Execute the command
    os.system(prospect_ddl_cmd)
    os.system(prospect_load_cmd)

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

    # Create function to get marketing_nameplate
    marketing_nameplate_function_definition_ddl = """
    DELIMITER //
    CREATE OR REPLACE FUNCTION get_marketing_template(net_worth NUMERIC, income NUMERIC,
        number_credit_cards NUMERIC, number_children NUMERIC, age NUMERIC,
        credit_rating NUMERIC, number_cars NUMERIC)
        RETURNS CHAR(100) AS
      DECLARE
        marketing_template CHAR(100) = '';
      BEGIN
        IF (net_worth>1000000 OR income>200000) THEN
          marketing_template =  CONCAT(marketing_template, '+HighValue');
        END IF;
        IF (number_credit_cards>5 OR number_children>3) THEN
          marketing_template =  CONCAT(marketing_template, '+Expenses');
        END IF;
        IF (age>45) THEN
          marketing_template =  CONCAT(marketing_template, '+Boomer');
        END IF;
        IF (credit_rating<600 or income <5000 or net_worth < 100000) THEN
          marketing_template =  CONCAT(marketing_template, '+MoneyAlert');
        END IF;
        IF (number_cars>3 or number_credit_cards>7) THEN
          marketing_template =  CONCAT(marketing_template, '+Spender');
        END IF;
        IF (age>25 or net_worth>1000000) THEN
          marketing_template =  CONCAT(marketing_template, '+Inherited');
        END IF;
        RETURN RIGHT(marketing_template,character_length(marketing_template)-1);
      END //
    DELIMITER ;
    """
    marketing_nameplate_function_definition_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+marketing_nameplate_function_definition_ddl+"\""
    os.system(marketing_nameplate_function_definition_cmd)
    
    load_prospect_query = """
    INSERT INTO Prospect
    SELECT SP.AGENCY_ID, (SELECT b_d.batch_date FROM batch_date b_d WHERE b_d.batch_number=%s) SK_RecordDateID,
          (SELECT b_d.batch_date FROM batch_date b_d WHERE b_d.batch_number=%s) SK_UpdateDateID, %s, FALSE, SP.LAST_NAME,
          SP.FIRST_NAME, SP.MIDDLE_INITIAL, SP.GENDER, SP.ADDRESS_LINE_1, SP.ADDRESS_LINE_2, SP.POSTAL_CODE, SP.CITY,
          SP.STATE, SP.COUNTRY, SP.PHONE, SP.INCOME, SP.NUMBER_CARS,SP.NUMBER_CHILDREM, SP.MARITAL_STATUS, SP.AGE,
          SP.CREDIT_RATING, SP.OWN_OR_RENT_FLAG, SP.EMPLOYER,SP.NUMBER_CREDIT_CARDS, SP.NET_WORTH, (SELECT get_marketing_template(SP.NET_WORTH, SP.INCOME, SP.NUMBER_CREDIT_CARDS, SP.NUMBER_CHILDREM, SP.AGE,
                      SP.CREDIT_RATING, SP.NUMBER_CARS) ) MarketingNameplate
    FROM S_Prospect SP;

    INSERT INTO DImessages
	    SELECT current_timestamp(),%s,'Prospect', 'Inserted rows', 'Status', (SELECT COUNT(*) FROM Prospect);
    """%(str(self.batch_number),str(self.batch_number),str(self.batch_number),str(self.batch_number))
    load_prospect_cmd = prospect_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+load_prospect_query+"\""
    os.system(load_prospect_cmd)
  
  def load_audit(self):
    """
    Create Audit table in the staging database and then load rows in files with "_audit.csv" ending into it.
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

  def load_staging_finwire(self):
    """
    Create S_Company and S_Security table in the staging database and then load rows in FINWIRE files with the type of CMP
    """

    # Create ddl to store finwire
    finwire_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE S_Company (
      PTS CHAR(15) NOT NULL,
      REC_TYPE CHAR(3) NOT NULL,
			COMPANY_NAME CHAR(60) NOT NULL,
			CIK CHAR(10) NOT NULL,
      STATUS CHAR(4) NOT NULL,
      INDUSTRY_ID CHAR(2) NOT NULL,
			SP_RATING CHAR(4) NOT NULL,
			FOUNDING_DATE CHAR(8) NOT NULL,
      ADDR_LINE_1 CHAR(80) NOT NULL,
      ADDR_LINE_2 CHAR(80) NOT NULL,
			POSTAL_CODE CHAR(12) NOT NULL,
			CITY CHAR(25) NOT NULL,
      STATE_PROVINCE CHAR(20) NOT NULL,
      COUNTRY CHAR(24) NOT NULL,
			CEO_NAME CHAR(46) NOT NULL,
			DESCRIPTION CHAR(150) NOT NULL
    );

    CREATE TABLE S_Security (
      PTS CHAR(15) NOT NULL,
      REC_TYPE CHAR(3) NOT NULL,
			SYMBOL CHAR(15) NOT NULL,
			ISSUE_TYPE CHAR(6) NOT NULL,
      STATUS CHAR(4) NOT NULL,
      NAME CHAR(70) NOT NULL,
			EX_ID CHAR(6) NOT NULL,
			SH_OUT CHAR(13) NOT NULL,
      FIRST_TRADE_DATE CHAR(8) NOT NULL,
      FIRST_TRADE_EXCHANGE CHAR(8) NOT NULL,
			DIVIDEN CHAR(12) NOT NULL,
			COMPANY_NAME_OR_CIK CHAR(60) NOT NULL
    );

    CREATE TABLE S_Financial(
      PTS CHAR(15),
      REC_TYPE CHAR(3),
      YEAR CHAR(4),
      QUARTER CHAR(1),
      QTR_START_DATE CHAR(8),
      POSTING_DATE CHAR(8),
      REVENUE CHAR(17),
      EARNINGS CHAR(17),
      EPS CHAR(12),
      DILUTED_EPS CHAR(12),
      MARGIN CHAR(12),
      INVENTORY CHAR(17),
      ASSETS CHAR(17),
      LIABILITIES CHAR(17),
      SH_OUT CHAR(13),
      DILUTED_SH_OUT CHAR(13),
      CO_NAME_OR_CIK CHAR(60)
    );
    """

    finwire_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+finwire_ddl+"\""
    os.system(finwire_ddl_cmd)


    
    base_path = "staging/"+self.sf+"/Batch1/"
    s_company_base_query = "INSERT INTO S_Company VALUES "
    s_security_base_query = "INSERT INTO S_Security VALUES "
    s_financial_base_query = "INSERT INTO S_Financial VALUES"
    s_company_values = []
    s_security_values = []
    s_financial_values = []
    max_packet = 150
    for fname in os.listdir(base_path):
      if("FINWIRE" in fname and "audit" not in fname):
        with open(base_path+fname, 'r') as finwire_file:
          for line in finwire_file:
            pts = line[:15] #0
            rec_type=line[15:18] #1

            if rec_type=="CMP":
              company_name = line[18:78] #2
              cik = line[78:88] #3
              status = line[88:92] #4
              industry_id = line[92:94] #5
              sp_rating = line[94:98] # 6
              founding_date = line[98:106] #7
              addr_line_1 = line[106:186] #8
              addr_line_2 = line[186:266] #9
              postal_code = line[266:278] #10
              city = line[278:303] #10
              state_province = line[303:323] #11
              country = line[323:347] #12
              ceo_name = line[347:393] #13
              description = line[393:] #14

              s_company_values.append("('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(pts,rec_type,company_name,cik,status,industry_id,sp_rating,founding_date,addr_line_1,addr_line_2,postal_code,city,state_province,country,ceo_name,description))

              if len(s_company_values)>=max_packet:
                # Create query to load text data into tradeType table
                s_company_load_query=s_company_base_query+','.join(s_company_values)
                s_company_values = []
                # Construct mysql client bash command to execute ddl and data loading query
                s_company_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+s_company_load_query+"\""
      
                # Execute the command
                os.system(s_company_load_cmd)
            elif rec_type == "SEC":
              symbol = line[18:33]
              issue_type = line[33:39]
              status = line[39:43]
              name = line[43:113]
              ex_id = line[113:119]
              sh_out = line[119:132]
              first_trade_date = line[132:140]
              first_trade_exchange = line[140:148]
              dividen = line[148:160]
              company_name = line[160:] 
              
              s_security_values.append("('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(pts,rec_type,symbol,issue_type,status,name,ex_id,sh_out,first_trade_date,first_trade_exchange,dividen,company_name))

              if len(s_security_values)>=max_packet:
                # Create query to load text data into tradeType table
                s_security_load_query=s_security_base_query+','.join(s_security_values)
                s_security_values = []
                # Construct mysql client bash command to execute ddl and data loading query
                s_security_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+s_security_load_query+"\""
      
                # Execute the command
                os.system(s_security_load_cmd)
            elif rec_type == "FIN":
              year = line[18:22]
              quarter = line[22:23]
              qtr_start_date = line[23:31]
              posting_date = line[31:39]
              revenue = line[39:56]
              earnings = line[56:73]
              eps = line[73:85]
              diluted_eps = line[85:97]
              margin = line[97:109]
              inventory = line[109:126]
              assets = line[126:143]
              liabilities = line[143:160]
              sh_out = line[160:173]
              diluted_sh_out = line[173:186]
              co_name_or_cik = line[186:]

              s_financial_values.append("('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(pts, rec_type, year,quarter,qtr_start_date,posting_date, revenue, earnings, eps, diluted_eps, margin, inventory, assets, liabilities, sh_out,diluted_sh_out,co_name_or_cik))

              if len(s_financial_values)>=max_packet:
                # Create query to load text data into tradeType table
                s_financial_load_query=s_financial_base_query+','.join(s_financial_values)
                s_financial_values = []
                # Construct mysql client bash command to execute ddl and data loading query
                s_financial_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+s_financial_load_query+"\""
      
                # Execute the command
                os.system(s_financial_load_cmd)

  def load_target_dim_company(self):
    """
    Create Dim Company table in the staging database and then load rows by joining staging_company, staging_industry, and staging StatusType
    """

    # Create ddl to store tradeType
    dim_company_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE DimCompany (
        SK_CompanyID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
        CompanyID INTEGER NOT NULL,
        Status CHAR(10) Not NULL,
        Name CHAR(60) Not NULL,
        Industry CHAR(50) Not NULL,
        SPrating CHAR(4),
        isLowGrade BOOLEAN,
        CEO CHAR(100) Not NULL,
        AddressLine1 CHAR(80),
        AddressLine2 CHAR(80),
        PostalCode CHAR(12) Not NULL,
        City CHAR(25) Not NULL,
        StateProv CHAR(20) Not NULL,
        Country CHAR(24),
        Description CHAR(150) Not NULL,
        FoundingDate DATE,
        IsCurrent BOOLEAN Not NULL,
        BatchID numeric(5) Not NULL,
        EffectiveDate DATE Not NULL,
        EndDate DATE Not NULL
      );
    """

    # Create query to load text data into dim_company table
    dim_company_load_query="""
      INSERT INTO DimCompany (CompanyID,Status,Name,Industry,SPrating,isLowGrade,CEO,AddressLine1,AddressLine2,PostalCode,City,StateProv,Country,Description,FoundingDate,IsCurrent,BatchID,EffectiveDate,EndDate)
      SELECT C.CIK,S.ST_NAME, C.COMPANY_NAME, I.IN_NAME,C.SP_RATING, IF(LEFT(C.SP_RATING,1)='A' OR LEFT (C.SP_RATING,3)='BBB','FALSE','TRUE'),
            C.CEO_NAME, C.ADDR_LINE_1,C.ADDR_LINE_2, C.POSTAL_CODE, C.CITY, C.STATE_PROVINCE, C.COUNTRY, C.DESCRIPTION,
            STR_TO_DATE(FOUNDING_DATE,'%Y%m%d'),TRUE, 1, STR_TO_DATE(LEFT(C.PTS,8),'%Y%m%d'), STR_TO_DATE('99991231','%Y%m%d')
      FROM S_Company C
      JOIN Industry I ON C.INDUSTRY_ID = I.IN_ID
      JOIN StatusType S ON C.STATUS = S.ST_ID;
    """
    
    # Construct mysql client bash command to execute ddl and data loading query
    dim_company_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+dim_company_ddl+"\""
    dim_company_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+dim_company_load_query+"\""
    
    # Execute the command
    os.system(dim_company_ddl_cmd)
    os.system(dim_company_load_cmd)    
  
  def load_target_dim_security(self):
    """
    Create Security table in the staging database and then load rows by ..
    """

    # Create ddl to store tradeType
    security_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE DimSecurity( SK_SecurityID INTEGER Not NULL PRIMARY KEY AUTO_INCREMENT,
      Symbol CHAR(15) Not NULL,
      Issue CHAR(6) Not NULL,
      Status CHAR(10) Not NULL,
      Name CHAR(70) Not NULL,
      ExchangeID CHAR(6) Not NULL,
      SK_CompanyID INTEGER Not NULL,
      SharesOutstanding INTEGER Not NULL,
      FirstTrade DATE Not NULL,
      FirstTradeOnExchange DATE Not NULL,
      Dividend INTEGER Not NULL,
      IsCurrent BOOLEAN Not NULL,
      BatchID numeric(5) Not NULL,
      EffectiveDate DATE Not NULL,
      EndDate DATE Not NULL
    );
    """

    # Create query to load text data into security table
    security_load_query="""
    INSERT INTO DimSecurity (Symbol,Issue,Status,Name,ExchangeID,SK_CompanyID,SharesOutstanding,FirstTrade,FirstTradeOnExchange,Dividend,IsCurrent,BatchID,EffectiveDate,EndDate)
    SELECT SS.SYMBOL,SS.ISSUE_TYPE, ST.ST_NAME, SS.NAME, SS.EX_ID, DC.SK_CompanyID, SS.SH_OUT, STR_TO_DATE(SS.FIRST_TRADE_DATE,'%Y%m%d'),
          STR_TO_DATE(FIRST_TRADE_EXCHANGE, '%Y%m%d'), SS.DIVIDEN, TRUE, 1, STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d'), STR_TO_DATE('99991231','%Y%m%d')
    FROM S_Security SS
    JOIN StatusType ST ON SS.STATUS = ST.ST_ID
    JOIN DimCompany DC ON DC.SK_CompanyID = convert(SS.COMPANY_NAME_OR_CIK, SIGNED)
                        AND DC.EffectiveDate <= STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d')
                        AND STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d') < DC.EndDate
                        AND LEFT(SS.COMPANY_NAME_OR_CIK,1)='0';

    INSERT INTO DimSecurity (Symbol,Issue,Status,Name,ExchangeID,SK_CompanyID,SharesOutstanding,FirstTrade,FirstTradeOnExchange,Dividend,IsCurrent,BatchID,EffectiveDate,EndDate)
    SELECT SS.SYMBOL,SS.ISSUE_TYPE, ST.ST_NAME, SS.NAME, SS.EX_ID, DC.SK_CompanyID, SS.SH_OUT, STR_TO_DATE(SS.FIRST_TRADE_DATE,'%Y%m%d'),
          STR_TO_DATE(FIRST_TRADE_EXCHANGE, '%Y%m%d'), SS.DIVIDEN, TRUE, 1, STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d'), STR_TO_DATE('99991231','%Y%m%d')
    FROM S_Security SS
    JOIN StatusType ST ON SS.STATUS = ST.ST_ID
    JOIN DimCompany DC ON RTRIM(SS.COMPANY_NAME_OR_CIK) = DC.Name
                        AND DC.EffectiveDate <= STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d')
                        AND STR_TO_DATE(LEFT(SS.PTS,8),'%Y%m%d') < DC.EndDate
                        AND LEFT(SS.COMPANY_NAME_OR_CIK,1) <> '0';
    """
    
    # Construct mysql client bash command to execute ddl and data loading query
    dim_security_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+security_ddl+"\""
    dim_security_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+security_load_query+"\""
    
    # Execute the command
    os.system(dim_security_ddl_cmd)
    os.system(dim_security_load_cmd)

  def load_target_financial(self):
    """
    Create Financial table in the staging database and then load rows by ..
    """

    # Create ddl to store tradeType
    financial_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE Financial (
      SK_CompanyID INTEGER Not NULL,
      FI_YEAR numeric(4) Not NULL,
      FI_QTR numeric(1) Not NULL,
      FI_QTR_START_DATE DATE Not NULL,
      FI_REVENUE numeric(15,2) Not NULL,
      FI_NET_EARN numeric(15,2) Not NULL,
      FI_BASIC_EPS numeric(10,2) Not NULL,
      FI_DILUT_EPS numeric(10,2) Not NULL,
      FI_MARGIN numeric(10,2) Not NULL,
      FI_INVENTORY numeric(15,2) Not NULL,
      FI_ASSETS numeric(15,2) Not NULL,
      FI_LIABILITY numeric(15,2) Not NULL,
      FI_OUT_BASIC numeric(12) Not NULL,
      FI_OUT_DILUT numeric(12) Not NULL
      );
    """

    # Create query to load text data into financial table
    financial_load_query="""
    INSERT INTO Financial
      SELECT SK_CompanyID, SF.YEAR, SF.QUARTER, SF.QTR_START_DATE, SF.REVENUE,  SF.EARNINGS, SF.EPS, SF.DILUTED_EPS,SF.MARGIN, SF.INVENTORY, SF.ASSETS, SF.LIABILITIES, SF.SH_OUT, SF.DILUTED_SH_OUT
      FROM S_Financial SF
      JOIN DimCompany DC ON DC.SK_CompanyID = convert(SF.CO_NAME_OR_CIK, SIGNED)
                          AND DC.EffectiveDate <= STR_TO_DATE(LEFT(SF.PTS,8),'%Y%m%d')
                          AND STR_TO_DATE(LEFT(SF.PTS,8),'%Y%m%d') < DC.EndDate
                          AND LEFT(CO_NAME_OR_CIK,1)='0';
    INSERT INTO Financial
      SELECT SK_CompanyID, SF.YEAR, SF.QUARTER, SF.QTR_START_DATE, SF.REVENUE,  SF.EARNINGS, SF.EPS, SF.DILUTED_EPS,SF.MARGIN, SF.INVENTORY, SF.ASSETS, SF.LIABILITIES, SF.SH_OUT, SF.DILUTED_SH_OUT
      FROM S_Financial SF
      JOIN DimCompany DC ON RTRIM(SF.CO_NAME_OR_CIK) = DC.Name
                          AND DC.EffectiveDate <= STR_TO_DATE(LEFT(SF.PTS,8),'%Y%m%d')
                          AND STR_TO_DATE(LEFT(SF.PTS,8),'%Y%m%d') < DC.EndDate
                          AND LEFT(CO_NAME_OR_CIK,1) <> '0'
    """
    
    # Construct mysql client bash command to execute ddl and data loading query
    dim_financial_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+financial_ddl+"\""
    dim_financial_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" --local-infile=1 -D "+self.db_name+" -e \""+financial_load_query+"\""
    
    # Execute the command
    os.system(dim_financial_ddl_cmd)
    os.system(dim_financial_load_cmd)    

