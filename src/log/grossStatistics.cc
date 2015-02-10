/*
 * grossStatistics.cpp
 *
 *  Created on: Nov 20, 2014
 *      Author: sivaprakash
 */
#include "squid.h"
#include <string>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include "log/DBConnection.h"
#include "log/RowData.h"
#include "log/RowDataDenied.h"
#include "log/grossStatistics.h"
#include "log/UserStatistics.h"
#include "log/DomainStatistics.h"


//void updateRowDataAcc(ResultSet *dailyRes,ResultSet *ymRes,PreparedStatement *pstmt);
//void insertRowDataAcc(ResultSet *ymRes,PreparedStatement *pstmt);

//void updateRowDataDen(ResultSet *dailyRes,ResultSet *ymRes,PreparedStatement *pstmt);
//void insertRowDataDen(ResultSet *ymRes,PreparedStatement *pstmt);


//void *grossStatisticsAcc(void *tbNa)
void grossStatisticsAcc(string tbNa)
{
	string tName = tbNa;

	cout<<"";
	string tn;


	try
	{
	/*	ifstream confFile("/home/sivaprakash/workspace/StatisticsDataFromDB/src/tabAcc.conf");
		confFile>>tName;
		confFile.close();
		cout<<"start of Acc thread for table:"<<tName<<endl;*/
		//syslog(LOG_NOTICE,"gsACC: start");
		PreparedStatement *readPstmt;
		ResultSet *dailyRes,*ymRes;

		string year = tName.substr(13,4);
		string month = tName.substr(10,2);
		string day = tName.substr(7,2);

		string yearStatisticstable = "ud_acc_"+year;
		string monthStatisticstable = "ud_acc_"+month;
		string schema = "squidStatistics_"+year;

		DBConnection *grossLog = new DBConnection();
		grossLog->dbConnOpen("127.0.0.1","3306","root","simple",schema);

		Statement *stmt = grossLog->conn->createStatement();

		checkPresenecOfGrossStatisticsTableAcc(stmt,yearStatisticstable);
		checkPresenecOfGrossStatisticsTableAcc(stmt,monthStatisticstable);

		string searchQueryMonth = "select * from "+ monthStatisticstable +"  where user=? and domain=? ;";
		string searchQueryYear = "select * from "+ yearStatisticstable +"  where user=? and domain=?;";


		string selectQuery = "select * from " + tName +";";
		readPstmt = grossLog->conn->prepareStatement(selectQuery);
		dailyRes = readPstmt->executeQuery();

		while(dailyRes->next())
		{
	//		cout<<dailyRes->getInt(4)<<"\tstart\n";
			for(int i=0;i<2;i++)
			{
				if(i ==  0)  //updating monthly gross statistics
				{
					tn = monthStatisticstable;
					readPstmt = grossLog->conn->prepareStatement(searchQueryMonth);
				}
				else
				{
					tn = yearStatisticstable;
					readPstmt = grossLog->conn->prepareStatement(searchQueryYear);
				}

				readPstmt->setString(1,dailyRes->getString(1));
				readPstmt->setString(2,dailyRes->getString(2));
				ymRes = readPstmt->executeQuery();

				if(ymRes->next())
				{
					updateRowDataAcc(dailyRes,ymRes,stmt,tn);
				}
				else
				{
					insertRowDataAcc(dailyRes,stmt,tn);
				}
			}
		}
		createUserStatisticsAcc(tName);
		createDomainStatisticsAcc(tName);
		 //syslog(LOG_NOTICE,"gsACC: end");
	}
	catch (sql::SQLException &e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
	catch (exception& e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}

}

void updateRowDataAcc(ResultSet *dailyRes,ResultSet *ymRes,Statement *stmt,string tn)
{
	try
	{
		RowData *rowData = new RowData();
		rowData->user = dailyRes->getString(1);
		rowData->domain = dailyRes->getString(2);
		rowData->size = dailyRes->getDouble(3) + ymRes->getDouble(3);
		rowData->connection = dailyRes->getInt(4) + ymRes->getInt(4);
		rowData->hit = dailyRes->getDouble(5) + ymRes->getDouble(5);
		rowData->miss = dailyRes->getDouble(6) + ymRes->getDouble(6);
		rowData->response_time = dailyRes->getDouble(7) + ymRes->getDouble(7);

		updateTableAcc(rowData,stmt,tn);
	}
	catch (sql::SQLException &e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
}
void insertRowDataAcc(ResultSet *dailyRes,Statement *stmt,string tn)
{
	try
	{
		RowData *rowData = new RowData();
		rowData->user = dailyRes->getString(1);
		rowData->domain = dailyRes->getString(2);
		rowData->size = dailyRes->getDouble(3);
		rowData->connection = dailyRes->getInt(4);
		rowData->hit = dailyRes->getDouble(5) ;
		rowData->miss = dailyRes->getDouble(6);
		rowData->response_time = dailyRes->getDouble(7);

		insertIntoTableAcc(rowData,stmt,tn);
	}
	catch (sql::SQLException &e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
}

void checkPresenecOfGrossStatisticsTableAcc(Statement *stmt,string tName)
{
	try
	{
		stmt->execute("create table if not exists " + tName + "(user varchar(16),domain varchar(100), size double, connection double, hit double, miss double,response_time double);");
	}
	catch (sql::SQLException &e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
}


//void *grossStatisticsDen(void *tbNa)
void grossStatisticsDen(string tbNa)
{

	//	string te = (char*)tbNa;
	//	cout<<"Den decryption:"<<te<<endl;
	string tName = tbNa;
	string tn;
	try
	{
	/*	ifstream confFile("/home/sivaprakash/workspace/StatisticsDataFromDB/src/tabDen.conf");
		confFile>>tName;
		confFile.close();*/

//		syslog(LOG_NOTICE,"gsDen:start");
		PreparedStatement *readPstmt;
		ResultSet *dailyRes,*ymRes;

		string year = tName.substr(13,4);
		string month = tName.substr(10,2);
		string day = tName.substr(7,2);


		string yearStatisticstable = "ud_den_"+year;
		string monthStatisticstable = "ud_den_"+month;
		string schema = "squidStatistics_"+year;

		DBConnection *grossLog = new DBConnection();
		grossLog->dbConnOpen("127.0.0.1","3306","root","simple",schema);

		Statement *stmt = grossLog->conn->createStatement();


		checkPresenecOfGrossStatisticsTableDen(stmt,yearStatisticstable);
		checkPresenecOfGrossStatisticsTableDen(stmt,monthStatisticstable);

		string searchQueryMonth = "select * from "+ monthStatisticstable +"  where user=? and domain=? ;";
		string searchQueryYear = "select * from "+ yearStatisticstable +"  where user=? and domain=?;";


		string selectQuery = "select * from " + tName +";";
		readPstmt = grossLog->conn->prepareStatement(selectQuery);
		dailyRes = readPstmt->executeQuery();



		while(dailyRes->next())
		{
			for(int i=0;i<2;i++)
			{
				if(i ==  0)
				{
					tn = monthStatisticstable;
					readPstmt = grossLog->conn->prepareStatement(searchQueryMonth);
				}
				else
				{
					tn = yearStatisticstable;
					readPstmt = grossLog->conn->prepareStatement(searchQueryYear);
				}

				readPstmt->setString(1,dailyRes->getString(1));
				readPstmt->setString(2,dailyRes->getString(2));
				ymRes = readPstmt->executeQuery();

				if(ymRes->next())
				{
					updateRowDataDen(dailyRes,ymRes,stmt,tn);
				}
				else
				{
					insertRowDataDen(dailyRes,stmt,tn);
				}
			}
		}
		createUserStatisticsDen(tName);
		createDomainStatisticsDen(tName);
//		 syslog(LOG_NOTICE,"gsDen:end");

	}
	catch (sql::SQLException &e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
	}
	catch (exception& e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
}

void updateRowDataDen(ResultSet *dailyRes,ResultSet *ymRes,Statement *stmt,string tn)
{
	try
	{
		RowDataDenied *rowData = new RowDataDenied();
		rowData->user = dailyRes->getString(1);
		rowData->domain = dailyRes->getString(2);
		rowData->connection = dailyRes->getInt(3) + ymRes->getInt(3);

		updateTableDen(rowData,stmt,tn);
	}
	catch (sql::SQLException &e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
}
void insertRowDataDen(ResultSet *dailyRes,Statement *stmt,string tn)
{
	try
	{
		RowDataDenied *rowData = new RowDataDenied();
		rowData->user = dailyRes->getString(1);
		rowData->domain = dailyRes->getString(2);
		rowData->connection = dailyRes->getInt(3);
		insertIntoTableDen(rowData,stmt,tn);
	}
	catch (sql::SQLException &e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
}

void checkPresenecOfGrossStatisticsTableDen(Statement *stmt,string tName)
{
	stmt->execute("create table if not exists " + tName + "(user varchar(16),domain varchar(100), connection double);");
}
