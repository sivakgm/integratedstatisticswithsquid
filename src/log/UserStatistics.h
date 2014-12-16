/*
 * UserStatistics.h
 *
 *  Created on: Nov 21, 2014
 *      Author: sivaprakash
 */

#ifndef USERSTATISTICS_H_
#define USERSTATISTICS_H_

#include <string>
#include <fstream>
#include <cstdlib>
#include "log/DBConnection.h"


void createUserStatisticsAcc(string tableName);
void insertDataIntoDailyUserStatisticsAcc(RowData *rowData,Statement *stmt,string tableName);
void checkPresenceOfUserDataInTableAcc(RowData *rowData,Statement *stmt,string tableName);
void checkPresenecOfUserStatisticsTableAcc(Statement *stmt,string tableName);

void createUserStatisticsDen(string tableName);
void insertDataIntoDailyUserStatisticsDen(RowDataDenied *rowDataDenied,Statement *stmt,string tableName);
void checkPresenceOfUserDataInTableDen(RowDataDenied *rowDataDenied,Statement *stmt,string tableName);
void checkPresenecOfUserStatisticsTableDen(Statement *stmt,string tableName);


/*namespace boost {

class UserStatistics {
public:
	UserStatistics();
	virtual ~UserStatistics();
};

}  namespace boost */

#endif /* USERSTATISTICS_H_ */
