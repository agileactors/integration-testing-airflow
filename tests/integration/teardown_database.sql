use testncintegration;
drop table IF EXISTS ncproject.fintransacts;
drop table IF EXISTS ncproject.ingestions;
drop user IF EXISTS testncuser;

IF SUSER_ID('testnclogin') IS NOT NULL
	drop login testnclogin;

drop schema IF EXISTS ncproject;

use master;
drop database IF EXISTS testncintegration;