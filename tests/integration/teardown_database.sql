revoke all PRIVILEGES on ALL TABLES IN schema fooproject FROM dataengineer;
revoke USAGE ON SCHEMA fooproject FROM dataengineer ;
REVOKE all PRIVILEGES ON DATABASE financedb FROM dataengineer;
drop user dataengineer;
drop table if exists fooproject.fintransacts;
drop schema if exists fooproject;