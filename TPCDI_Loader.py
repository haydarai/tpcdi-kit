import os
import glob

class TPCDI_Loader():
  BASE_MYSQL_CMD = ""

  def __init__(self, sf, db_name, config):
    """
    Initialize staging database.

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

  def load_staging_dimDate(self):
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

  def load_staging_dimTime(self):
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

  def load_staging_Industry(self):
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

  def load_staging_statusType(self):
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

  def load_staging_taxRate(self):
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
  
  def load_staging_tradeType(self):
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
    """

    finwire_ddl_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+finwire_ddl+"\""
    os.system(finwire_ddl_cmd)


    
    base_path = "staging/"+self.sf+"/Batch1/"
    s_company_base_query = "INSERT INTO S_Company VALUES "
    s_security_base_query = "INSERT INTO S_Security VALUES "
    s_company_values = []
    s_security_values = []
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
              company_name = line[160:220] 
              
              s_security_values.append("('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(pts,rec_type,symbol,issue_type,status,name,ex_id,sh_out,first_trade_date,first_trade_exchange,dividen,company_name))

              if len(s_security_values)>=max_packet:
                # Create query to load text data into tradeType table
                s_security_load_query=s_security_base_query+','.join(s_security_values)
                s_security_values = []
                # Construct mysql client bash command to execute ddl and data loading query
                s_security_load_cmd = TPCDI_Loader.BASE_MYSQL_CMD+" -D "+self.db_name+" -e \""+s_security_load_query+"\""
      
                # Execute the command
                os.system(s_security_load_cmd)

  def load_target_dim_company(self):
    """
    Create Dim Company table in the staging database and then load rows by joining staging_company, staging_industry, and staging StatusType
    """

    # Create ddl to store tradeType
    dim_company_ddl = """
    USE """+self.db_name+""";

    CREATE TABLE DimCompany (
        SK_CompanyID INTEGER NOT NULL,
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
      INSERT INTO DimCompany
      SELECT C.CIK, C.CIK,C.COMPANY_NAME,S.ST_NAME, I.IN_NAME,C.SP_RATING, IF(LEFT(C.SP_RATING,1)='A' OR LEFT (C.SP_RATING,3)='BBB','FALSE','TRUE'),
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
  