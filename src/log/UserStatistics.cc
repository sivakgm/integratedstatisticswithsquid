/*
 * UserStatistics.cpp
 *
 *  Created on: Nov 21, 2014
 *      Author: sivaprakash
 */



#include "log/UserStatistics.h"
#include "log/RowData.h"
#include "log/RowDataDenied.h"


void createUserStatisticsAcc(string tableName)
{
	try
	{
		string userQuery = "select * from "+ tableName + " order by user;";

		PreparedStatement *readPstmt;
		ResultSet *udStatData;

		string year = tableName.substr(13,4);
		string month = tableName.substr(10,2);
		string day = tableName.substr(7,2);

		string yearStatisticstable = "u_acc_"+year;
		string monthStatisticstable = "u_acc_"+month;
		string dayStatisticstable = "u_acc_"+day+"_"+month+"_"+year;
		string schema = "squidStatistics_"+year;

		DBConnection *grossLog = new DBConnection();
		grossLog->dbConnOpen("127.0.0.1","3306","root","simple",schema);

		Statement *stmt = grossLog->conn->createStatement();

		string user="";
		RowData *rowData = new RowData();

		checkPresenecOfUserStatisticsTableAcc(stmt,dayStatisticstable);
		checkPresenecOfUserStatisticsTableAcc(stmt,monthStatisticstable);
		checkPresenecOfUserStatisticsTableAcc(stmt,yearStatisticstable);

		readPstmt = grossLog->conn->prepareStatement(userQuery);
		udStatData = readPstmt->executeQuery();

		while(udStatData->next())
		{

			if(udStatData->getString(1) == user)
			{
				rowData->size = rowData->size + udStatData->getDouble(3);
				rowData->connection = rowData->connection + udStatData->getInt(4);
				rowData->hit = rowData->hit + udStatData->getDouble(5);
				rowData->miss = rowData->miss + udStatData->getDouble(6);
				rowData->response_time = rowData->response_time + udStatData->getDouble(7);
			}
			else
			{
				if(user != "")
				{
					insertDataIntoDailyUserStatisticsAcc(rowData,stmt,dayStatisticstable);
					checkPresenceOfUserDataInTableAcc(rowData,stmt,monthStatisticstable);

					checkPresenceOfUserDataInTableAcc(rowData,stmt,yearStatisticstable);
				}
				user = udStatData->getString(1);
				rowData->user = udStatData->getString(1);
				rowData->size = udStatData->getDouble(3);
				rowData->connection = udStatData->getInt(4);
				rowData->hit = udStatData->getDouble(5);
				rowData->miss = udStatData->getDouble(6);
				rowData->response_time = udStatData->getDouble(7);
			}

		}
		insertDataIntoDailyUserStatisticsAcc(rowData,stmt,dayStatisticstable);
		checkPresenceOfUserDataInTableAcc(rowData,stmt,monthStatisticstable);
		checkPresenceOfUserDataInTableAcc(rowData,stmt,yearStatisticstable);


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

void insertDataIntoDailyUserStatisticsAcc(RowData *rowData,Statement *stmt,string tableName)
{
	try
	{
		stmt->execute("insert into "+tableName+"(user,size,connection,hit,miss,response_time) values('"+rowData->user+"',"+boost::lexical_cast<std::string>(rowData->size)+","+boost::lexical_cast<std::string>(rowData->connection)+","+boost::lexical_cast<std::string>(rowData->hit)+","+boost::lexical_cast<std::string>(rowData->miss)+","+ boost::lexical_cast<std::string>(rowData->response_time)+");");
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

void checkPresenceOfUserDataInTableAcc(RowData *rowData,Statement *stmt,string tableName)
{
	try
	{
		ResultSet *res = stmt->executeQuery("select * from "+tableName+" where user='"+rowData->user+"';");


		if(res->next())
		{

			RowData *temp = new RowData();
			temp->size = rowData->size + res->getDouble(2);
			temp->connection = rowData->connection + res->getInt(3);
			temp->hit = rowData->hit + res->getDouble(4);
			temp->miss = rowData->miss + res->getDouble(5);
			temp->response_time = rowData->response_time + res->getDouble(6);
			stmt->execute("update "+ tableName +" set size="+boost::lexical_cast<std::string>(temp->size)+",connection=" + boost::lexical_cast<std::string>(temp->connection) + ",hit="+ boost::lexical_cast<std::string>(temp->hit) + ",miss=" + boost::lexical_cast<std::string>(temp->miss) + ",response_time=" + boost::lexical_cast<std::string>(temp->response_time) + " where user='" + rowData->user + "';");
		}
		else
		{
			insertDataIntoDailyUserStatisticsAcc(rowData,stmt,tableName);
		}
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

void checkPresenecOfUserStatisticsTableAcc(Statement *stmt,string tableName)
{
	try
	{
		stmt->execute("create table if not exists " + tableName + "(user varchar(16), size double, connection double, hit double, miss double,response_time double);");
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

void createUserStatisticsDen(string tableName)
{
	try
	{
		string userQuery = "select * from "+ tableName + " order by user;";

		PreparedStatement *readPstmt;
		ResultSet *udStatData;

		string year = tableName.substr(13,4);
		string month = tableName.substr(10,2);
		string day = tableName.substr(7,2);

		string yearStatisticstable = "u_den_"+year;
		string monthStatisticstable = "u_den_"+month;
		string dayStatisticstable = "u_den_"+day+"_"+month+"_"+year;

		string schema = "squidStatistics_"+year;

		DBConnection *grossLog = new DBConnection();
		grossLog->dbConnOpen("127.0.0.1","3306","root","simple",schema);

		Statement *stmt = grossLog->conn->createStatement();
		string user="";
		RowDataDenied *rowDataDenied = new RowDataDenied();

		checkPresenecOfUserStatisticsTableDen(stmt,dayStatisticstable);
		checkPresenecOfUserStatisticsTableDen(stmt,monthStatisticstable);
		checkPresenecOfUserStatisticsTableDen(stmt,yearStatisticstable);

		readPstmt = grossLog->conn->prepareStatement(userQuery);
		udStatData = readPstmt->executeQuery();

		while(udStatData->next())
		{
			if(udStatData->getString(1) == user)
			{
				rowDataDenied->connection = rowDataDenied->connection + udStatData->getInt(3);
			}
			else
			{
				if(user != "")
				{
					insertDataIntoDailyUserStatisticsDen(rowDataDenied,stmt,dayStatisticstable);
					checkPresenceOfUserDataInTableDen(rowDataDenied,stmt,monthStatisticstable);
					checkPresenceOfUserDataInTableDen(rowDataDenied,stmt,yearStatisticstable);
				}
				user = udStatData->getString(1);
				rowDataDenied->user = udStatData->getString(1);
				rowDataDenied->connection = udStatData->getInt(3);
			}

		}
		insertDataIntoDailyUserStatisticsDen(rowDataDenied,stmt,dayStatisticstable);
		checkPresenceOfUserDataInTableDen(rowDataDenied,stmt,monthStatisticstable);
		checkPresenceOfUserDataInTableDen(rowDataDenied,stmt,yearStatisticstable);
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

void insertDataIntoDailyUserStatisticsDen(RowDataDenied *rowDataDenied,Statement *stmt,string tableName)
{
	try
	{
		stmt->execute("insert into "+ tableName +"(user,connection) values('"+rowDataDenied->user+"'," + boost::lexical_cast<std::string>(rowDataDenied->connection) +");");
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

void checkPresenceOfUserDataInTableDen(RowDataDenied *rowDataDenied,Statement *stmt,string tableName)
{
	try
	{
		ResultSet *res = stmt->executeQuery("select * from "+tableName+" where user='"+rowDataDenied->user+"';");

		if(res->next())
		{
			RowDataDenied *temp = new RowDataDenied();
			temp->connection = rowDataDenied->connection + res->getInt(2);
			stmt->execute("update "+tableName+" set connection="+boost::lexical_cast<std::string>(temp->connection)+" where user='"+rowDataDenied->user+"';");
		}
		else
		{
			insertDataIntoDailyUserStatisticsDen(rowDataDenied,stmt,tableName);
		}
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

void checkPresenecOfUserStatisticsTableDen(Statement *stmt,string tableName)
{
	try
	{
		stmt->execute("create table if not exists " + tableName + "(user varchar(16), connection double);");
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

