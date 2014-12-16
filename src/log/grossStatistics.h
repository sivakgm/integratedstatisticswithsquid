/*
 * grossStatistics.h
 *
 *  Created on: Nov 20, 2014
 *      Author: sivaprakash
 */

#ifndef GROSSSTATISTICS_H_
#define GROSSSTATISTICS_H_

//void *grossStatisticsAcc(void *tbNa);
void grossStatisticsAcc(string tbNa);
void insertRowDataAcc(ResultSet *dRes,PreparedStatement *pstmt);
void updateRowDataAcc(ResultSet *dRes,ResultSet *ymRes,PreparedStatement *pstmt);
void checkPresenecOfGrossStatisticsTableAcc(Statement *stmt,string tableName);

//void *grossStatisticsDen(void *tbNa);
void grossStatisticsDen(string tbNa);
void insertRowDataDen(ResultSet *dRes,PreparedStatement *pstmt);
void updateRowDataDen(ResultSet *dRes,ResultSet *ymRes,PreparedStatement *pstmt);
void checkPresenecOfGrossStatisticsTableDen(Statement *stmt,string tableName);


#endif /* GROSSSTATISTICS_H_ */
