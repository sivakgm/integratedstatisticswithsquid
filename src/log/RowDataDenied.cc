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
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
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
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
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
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
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
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
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
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
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
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
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
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
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
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
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
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}
}

void tempTableToDayTableDen(DBConnection *statLog,string currentTable,string dayTN)
{
		try
		{
		syslog(LOG_NOTICE,"tempTableToDayTableDen start");
   		ResultSet *dayRes,*temRes;
		PreparedStatement *readPstmt;
		string ctn = currentTable;
		string tn = dayTN;
                string searchQueryDay = "select * from "+ tn +"  where user=? and domain=?;";
                string selectQuery = "select * from " + ctn  +";";
		Statement *stmt = statLog->conn->createStatement();

                readPstmt = statLog->conn->prepareStatement(selectQuery);
                temRes = readPstmt->executeQuery();

                while(temRes->next())
                {
	
                      		readPstmt = statLog->conn->prepareStatement(searchQueryDay);
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
	}

        catch (exception& e)
        {	
		syslog(LOG_NOTICE,"Inside Temp Row Data Denied");
                syslog(LOG_NOTICE,e.what());
                cout << "# ERR File: " << __FILE__;
                cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
            cout << e.what() << '\n';
        }

}
