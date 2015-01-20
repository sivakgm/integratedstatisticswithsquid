/*
 * RowDataDenied.h
 *
 *  Created on: Nov 18, 2014
 *      Author: sivaprakash
 */

#ifndef ROWDATADENIED_H_
#define ROWDATADENIED_H_

#include <string>

#include <cppconn/statement.h>
#include <cppconn/resultset.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/exception.h>

#include <boost/lexical_cast.hpp>

class DBConnection;
struct logDataDen;

using namespace std;
using namespace sql;


class RowDataDenied {
public:
	string user;
	string domain;
	int connection;
	int priority;
	int isInTable;

	RowDataDenied();
};

void insertAllDenObjDataIntoTable(DBConnection *statLog,string ctn);
void updateDataInDenObj(DBConnection *statLog,RowDataDenied *rowDenData,logDataDen *log);
int checkDataInDenOBJ(int count,string user,string domain);
void setDenObjPriority(int lim);
int getLeastDenObjPriority();
void insertDenObjIntoTable(int pointObj,DBConnection *statLog,string ctn);
void emptyTheDenObj(int pointObj);
void updateDenObjFromTable(int pointObj,ResultSet *res);
void createNewDenObj();
void tempTableToDayTableDen(DBConnection *statLog,string currentTableDen,string dayTN);

#endif /* ROWDATADENIED_H_ */
