/*
 * RowDataDenied.cpp
 *
 *  Created on: Nov 18, 2014
 *      Author: sivaprakash
 */
#include "squid.h"
#include "log/RowDataDenied.h"
#include "log/DBConnection.h"


extern const int MAXDENIEDOBJ = 4;
extern int NoDENOBJ;
extern string currentTableDen;
extern RowDataDenied *rowDataDen[MAXDENIEDOBJ];


RowDataDenied::RowDataDenied()
{
	user = "";
	domain = "";
	connection = 0;
	priority = 0;
	isInTable = 0;
}


void updateDenObjFromTable(int pointObj,ResultSet *res)
{
	try
	{
		rowDataDen[pointObj]->user = res->getString(1);
		rowDataDen[pointObj]->domain = res->getString(2);
		rowDataDen[pointObj]->connection = res->getInt(3);
		rowDataDen[pointObj]->isInTable = 1;
	}
	catch (exception& e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
}

void insertDenObjIntoTable(int pointObj,DBConnection *statLog,string ctn)
{
	string tn = ctn;
	try
	{
		if(rowDataDen[pointObj]->isInTable == 1)
		{
			updateTableDen(rowDataDen[pointObj],statLog->stmt,tn);
		}
		else
		{
			insertIntoTableDen(rowDataDen[pointObj],statLog->stmt,tn);
		}
	}
	catch (exception& e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
}

void emptyTheDenObj(int pointObj)
{
	try
	{
		rowDataDen[pointObj]->user = "";
		rowDataDen[pointObj]->domain = "";
		rowDataDen[pointObj]->connection = 0;
		rowDataDen[pointObj]->isInTable = 0;
	}
	catch (exception& e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
}


int getLeastDenObjPriority()
{
	try
	{
		for(int i=0;i<NoDENOBJ;i++)
		{
			if(rowDataDen[i]->priority == NoDENOBJ )
				return i;
		}
	}
	catch (exception& e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
	return -1;
}

void setDenObjPriority(int lim)
{
	try
	{
		if(lim == 0)
		{
			lim = NoDENOBJ;
		}
/*		for(int i=0;i<lim;i++)
		{
			rowDataDen[i]->priority = rowDataDen[i]->priority + 1;
		}*/
		for(int i=0;i<NoDENOBJ;i++)
                {
                        if( rowDataDen[i]->priority < lim )
                        {
                                rowDataDen[i]->priority = rowDataDen[i]->priority + 1;
                        }
                }

		
	}
	catch (exception& e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
}

void updateDataInDenObj(DBConnection *statLog,RowDataDenied *rowdataden,logDataDen *log)
{
	try
	{
		int lim = rowdataden->priority;
		rowdataden->user = log->user;
		rowdataden->domain = log->domain;
		rowdataden->connection = rowdataden->connection + 1;
		rowdataden->priority = 0;

		insertIntoTableDenTime(rowdataden,log->tim,statLog->stmt,statLog->tableNameDenTime);
		setDenObjPriority(lim);
		return;
	}
	catch (exception& e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
}

int checkDataInDenOBJ(int count,string user,string domain)
{
	try
	{
	for(int i=0;i<count;i++)
		{
			if(rowDataDen[i]->user == user && rowDataDen[i]->domain == domain)
			{
				return i;
			}
		}
	}
	catch (exception& e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
	return -1;
}
void insertAllDenObjDataIntoTable(DBConnection *statLog,string ctn)
{
	string tn = ctn;
	try
	{
		for(int i=0;i<NoDENOBJ;i++)
		{
			insertDenObjIntoTable(i,statLog,tn);
		}
		for(int i=0;i<NoDENOBJ;i++)
		{
			delete rowDataDen[i];
		}
		NoDENOBJ = 0;
	}
	catch (exception& e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
}

void createNewDenObj()
{
	try
	{
		rowDataDen[NoDENOBJ] = new RowDataDenied();
		NoDENOBJ++;
		return;
	}
	catch (exception& e)
	{
	syslog(LOG_NOTICE,e.what());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}
}

void tempTableToDayTableDen(DBConnection *statLog,string currentTable,string dayTN)
{
		try
		{
		string ctn = currentTable;
		string tn = dayTN;
		syslog(LOG_NOTICE,"tempTableToDayTableDen start");
   		ResultSet *dayRes,*temRes;
		PreparedStatement *readPstmt;

		sql::Driver* drivers = get_driver_instance();
                sql::Connection* conns = drivers->connect("tcp://127.0.0.1:3306","root","simple");
                conns->setSchema("squidStatistics_2015");


                string searchQueryDay = "select * from "+ tn +"  where user=? and domain=?;";
                string selectQuery = "select * from " + ctn  +";";
		Statement *stmt = conns->createStatement();

                readPstmt = conns->prepareStatement(selectQuery);
                temRes = readPstmt->executeQuery();

                while(temRes->next())
                {
	
                      		readPstmt = conns->prepareStatement(searchQueryDay);
				readPstmt->setString(1,temRes->getString(1));
	                        readPstmt->setString(2,temRes->getString(2));
                                dayRes = readPstmt->executeQuery();

                                if(dayRes->next())
                                {
					RowDataDenied *rowData = new RowDataDenied();
			                rowData->user = temRes->getString(1);
			                rowData->domain = temRes->getString(2);
			                rowData->connection = temRes->getInt(3) + dayRes->getInt(3);
                			updateTableDen(rowData,stmt,tn);
                                }
                                else
                                {
					RowDataDenied *rowData = new RowDataDenied();
			                rowData->user = temRes->getString(1);
			                rowData->domain = temRes->getString(2);
			                rowData->connection = temRes->getInt(3);
			                insertIntoTableDen(rowData,stmt,tn);
                                }
               }
		syslog(LOG_NOTICE,"tempTableToDayTableDen end");
		delete readPstmt;
                delete temRes;
                delete dayRes;
                delete stmt;
                delete conns;

	}

        catch (exception& e)
        {	
		syslog(LOG_NOTICE,"Inside Temp Row Data Denied");
                syslog(LOG_NOTICE,e.what());
		syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
		syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
}

}
