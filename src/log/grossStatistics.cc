/*
 * grossStatistics.cpp
 *
 *  Created on: Nov 20, 2014
 *      Author: sivaprakash
 */

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


	try
	{
	/*	ifstream confFile("/home/sivaprakash/workspace/StatisticsDataFromDB/src/tabAcc.conf");
		confFile>>tName;
		confFile.close();
		cout<<"start of Acc thread for table:"<<tName<<endl;*/

		PreparedStatement *readPstmt,*inPstmt,*upPstmt;
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

		string insertMonth = "insert into " + monthStatisticstable + "(user,domain,size,connection,hit,miss,response_time) values(?,?,?,?,?,?,?);";
		string updateMonth = "update " + monthStatisticstable + " set size=?,connection=?,hit=?,miss=?,response_time=? where user=? and domain=?;";

		string insertYear = "insert into " + yearStatisticstable + "(user,domain,size,connection,hit,miss,response_time) values(?,?,?,?,?,?,?);";
		string updateYear = "update " + yearStatisticstable + " set size=?,connection=?,hit=?,miss=?,response_time=? where user=? and domain=?;";

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
					readPstmt = grossLog->conn->prepareStatement(searchQueryMonth);
					inPstmt =  grossLog->conn->prepareStatement(insertMonth);
					upPstmt = grossLog->conn->prepareStatement(updateMonth);
				}
				else
				{
					readPstmt = grossLog->conn->prepareStatement(searchQueryYear);
					inPstmt =  grossLog->conn->prepareStatement(insertYear);
					upPstmt = grossLog->conn->prepareStatement(updateYear);
				}

				readPstmt->setString(1,dailyRes->getString(1));
				readPstmt->setString(2,dailyRes->getString(2));
				ymRes = readPstmt->executeQuery();

				if(ymRes->next())
				{
					updateRowDataAcc(dailyRes,ymRes,upPstmt);
				}
				else
				{
					//	cout<<dailyRes->getInt(4)<<endl;
					insertRowDataAcc(dailyRes,inPstmt);
				}
			}
		}
		createUserStatisticsAcc(tName);
		createDomainStatisticsAcc(tName);
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}

}

void updateRowDataAcc(ResultSet *dailyRes,ResultSet *ymRes,PreparedStatement *pstmt)
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

		updateTableAcc(rowData,pstmt);
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}
void insertRowDataAcc(ResultSet *dailyRes,PreparedStatement *pstmt)
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

		insertIntoTableAcc(rowData,pstmt);
	}
	catch (sql::SQLException &e)
	{
	  cout << "# ERR: SQLException in " << __FILE__;
	  cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	  cout << "# ERR: " << e.what();
	  cout << " (MySQL error code: " << e.getErrorCode();
	  cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}

void checkPresenecOfGrossStatisticsTableAcc(Statement *stmt,string tName)
{
	try
	{
		stmt->execute("create table if not exists " + tName + "(user varchar(12),domain varchar(100), size double, connection int, hit float, miss float,response_time float);");
	}
	catch (sql::SQLException &e)
	{
	  cout << "# ERR: SQLException in " << __FILE__;
	  cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	  cout << "# ERR: " << e.what();
	  cout << " (MySQL error code: " << e.getErrorCode();
	  cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}


//void *grossStatisticsDen(void *tbNa)
void grossStatisticsDen(string tbNa)
{

	//	string te = (char*)tbNa;
	//	cout<<"Den decryption:"<<te<<endl;
	string tName = tbNa;
	try
	{
	/*	ifstream confFile("/home/sivaprakash/workspace/StatisticsDataFromDB/src/tabDen.conf");
		confFile>>tName;
		confFile.close();*/


		PreparedStatement *readPstmt,*inPstmt,*upPstmt;
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

		string insertMonth = "insert into " + monthStatisticstable + "(user,domain,connection) values(?,?,?);";
		string updateMonth = "update " + monthStatisticstable + " set connection=? where user=? and domain=?;";

		string insertYear = "insert into " + yearStatisticstable + "(user,domain,connection) values(?,?,?);";
		string updateYear = "update " + yearStatisticstable + " set connection=? where user=? and domain=?;";

		string selectQuery = "select * from " + tName +";";
		readPstmt = grossLog->conn->prepareStatement(selectQuery);
		dailyRes = readPstmt->executeQuery();



		while(dailyRes->next())
		{
			for(int i=0;i<2;i++)
			{
				if(i ==  0)
				{
					readPstmt = grossLog->conn->prepareStatement(searchQueryMonth);
					inPstmt =  grossLog->conn->prepareStatement(insertMonth);
					upPstmt = grossLog->conn->prepareStatement(updateMonth);
				}
				else
				{
					readPstmt = grossLog->conn->prepareStatement(searchQueryYear);
					inPstmt =  grossLog->conn->prepareStatement(insertYear);
					upPstmt = grossLog->conn->prepareStatement(updateYear);
				}

				readPstmt->setString(1,dailyRes->getString(1));
				readPstmt->setString(2,dailyRes->getString(2));
				ymRes = readPstmt->executeQuery();

				if(ymRes->next())
				{
					updateRowDataDen(dailyRes,ymRes,upPstmt);
				}
				else
				{
					insertRowDataDen(dailyRes,inPstmt);
				}
			}
		}
		createUserStatisticsDen(tName);
		createDomainStatisticsDen(tName);

	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}
}

void updateRowDataDen(ResultSet *dailyRes,ResultSet *ymRes,PreparedStatement *pstmt)
{
	try
	{
		RowDataDenied *rowData = new RowDataDenied();
		rowData->user = dailyRes->getString(1);
		rowData->domain = dailyRes->getString(2);
		rowData->connection = dailyRes->getInt(3) + ymRes->getInt(3);

		updateTableDen(rowData,pstmt);
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}
void insertRowDataDen(ResultSet *dailyRes,PreparedStatement *pstmt)
{
	try
	{
		RowDataDenied *rowData = new RowDataDenied();
		rowData->user = dailyRes->getString(1);
		rowData->domain = dailyRes->getString(2);
		rowData->connection = dailyRes->getInt(3);
		insertIntoTableDen(rowData,pstmt);
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}

void checkPresenecOfGrossStatisticsTableDen(Statement *stmt,string tName)
{
	stmt->execute("create table if not exists " + tName + "(user varchar(12),domain varchar(100), connection int);");
}
