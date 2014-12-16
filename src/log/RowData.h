/*
 * RowData.h
 *
 *  Created on: Nov 5, 2014
 *      Author: sivaprakash
 */


#ifndef ROWDATA_H_
#define ROWDATA_H_

#include <string>

#include <cppconn/statement.h>
#include <cppconn/resultset.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/exception.h>

#include <boost/lexical_cast.hpp>

class DBConnection;
struct logDataAcc;

using namespace std;
using namespace sql;


class RowData {
public:

	string user;
	string domain;
	float size;
	int connection;
	float hit;
	float miss;
	int priority;
	int isInTable;
	float response_time;

	RowData();
};


void insertAllObjDataIntoTable(DBConnection *statLog);
void setObjPriority(int lim);
void createNewObj();
void updateObjFromTable(int pointObj,ResultSet *res);
void emptyTheObj(int pointObj);
void insertObjIntoTable(int pointObj,DBConnection *statLog);
int getLeastObjPriority();
void readResSet(DBConnection *logDB);
int checkDataInOBJ(int count,string user,string domain);
void updateDataInObj(DBConnection *statLog,RowData *rowdata,logDataAcc *res);
int checkDataInTable(DBConnection *statLog,string tableName,string user,string domain);


#endif /* ROWDATA_H_ */
