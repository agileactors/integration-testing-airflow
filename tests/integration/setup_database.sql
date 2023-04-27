use testncintegration;

IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'ncproject' )
    EXEC('CREATE SCHEMA ncproject');

IF SUSER_ID('testnclogin') IS NULL
	create login testnclogin with password='ncuser123!!';

IF DATABASE_PRINCIPAL_ID('testncuser') IS NULL
	create user testncuser for login testnclogin with DEFAULT_SCHEMA = ncproject;

IF OBJECT_ID(N'ncproject.fintransacts', N'U') IS NULL
BEGIN
	create table ncproject.fintransacts(
	 id Int not Null,
	 last_transaction_date datetime,
	 description Varchar(200),
	 CONSTRAINT PK_FINTRANSACTS PRIMARY KEY (id)
	)
END;

IF OBJECT_ID(N'ncproject.ingestions', N'U') IS NULL
BEGIN
	create table ncproject.ingestions(
	 id Int not Null,
	 last_transaction_date datetime,
	 ingestion_date Varchar(200) not Null,
	 CONSTRAINT PK_INGESTIONS PRIMARY KEY (id)
	)
END;

Grant select on ncproject.fintransacts to testncuser;
Grant insert on ncproject.fintransacts to testncuser;
Grant update on ncproject.fintransacts to testncuser;
Grant delete on ncproject.fintransacts to testncuser;
Grant select on ncproject.ingestions to testncuser;
Grant insert on ncproject.ingestions to testncuser;
Grant update on ncproject.ingestions to testncuser;
Grant delete on ncproject.ingestions to testncuser;