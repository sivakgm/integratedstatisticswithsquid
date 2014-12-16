/*
 * DomainStatistics.h
 *
 *  Created on: Nov 21, 2014
 *      Author: sivaprakash
 */

#ifndef DOMAINSTATISTICS_H_
#define DOMAINSTATISTICS_H_


#include <string>
#include "log/DBConnection.h"

void createDomainStatisticsAcc(string tableName);
void insertDataIntoDailyDomainStatisticsAcc(RowData *rowData,Statement *stmt,string tableName);
void checkPresenceOfDomainDataInTableAcc(RowData *rowData,Statement *stmt,string tableName);
void checkPresenecOfDomainStatisticsTableAcc(Statement *stmt,string tableName);

void createDomainStatisticsDen(string tableName);
void insertDataIntoDailyDomainStatisticsDen(RowDataDenied *rowDataDenied,Statement *stmt,string tableName);
void checkPresenceOfDomainDataInTableDen(RowDataDenied *rowDataDenied,Statement *stmt,string tableName);
void checkPresenecOfDomainStatisticsTableDen(Statement *stmt,string tableName);




#endif /* DOMAINSTATISTICS_H_ */
