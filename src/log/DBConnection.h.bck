/*
 * DBConnection.h
 *
 *  Created on: Nov 5, 2014
 *      Author: sivaprakash
 */

#ifndef DBCONNECTION_H_
#define DBCONNECTION_H_




#include <iostream>
#include <string>

#include <mysql_connection.h>
#include <mysql_driver.h>

#include <cppconn/driver.h>
#include <cppconn/statement.h>
#include <cppconn/resultset.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/exception.h>

#include <boost/lexical_cast.hpp>

#include <ctime>
#include <sys/time.h>

class RowData;
class RowDataDenied;

using namespace std;
using namespace sql;


struct logDataAcc
{
	string tim;
	int size;
	string user;
	int response_time;
	string domain;
	string status;
};

struct logDataDen
{
	string tim;
	string user;
	string domain;
};


class DBConnection
{
public:

	Connection *conn;
	PreparedStatement *insPstmtAcc,*insPstmtDen,*insPstmtAccTime,*insPstmtDenTime;
	PreparedStatement *upPstmtAcc,*upPstmtDen;
	PreparedStatement *readpstmt;
	Statement *stmt;
	ResultSet *res;

	string tableName;
	string tableNameAcc,tableNameDen,tableNameAccTime,tableNameDenTime;
	string tableNameMonthAcc,tableNameYearAcc,tableNameMonthDen,tableNameYearDen;


	void dbConnOpen(string host,string port,string user,string pass,string schema);

	void setPstmt();
	void setReadPstmt(int a,string tableName,string user,string domain);
	void readTable();

	void createStatTableName(string tableName);
	void createTableIfNotExist();
	void createDBIfNotExists(string schema);
	void createStatTable(int flag,string tableName);
};

void insertIntoTableAcc(RowData *rowData,PreparedStatement *pstmt);
void updateTableAcc(RowData *rowData,PreparedStatement *pstmt);
void insertIntoTableDen(RowDataDenied *rowData,PreparedStatement *pstmt);
void updateTableDen(RowDataDenied *rowData,PreparedStatement *pstmt);
void insertIntoTableAccTime(RowData *rowData,string time,PreparedStatement *pstmt);
void insertIntoTableDenTime(RowDataDenied *rowData,string time,PreparedStatement *pstmt);

string parseURLtoDomain(string url);
string timeAndDate();

#endif /* DBCONNECTION_H_ */
