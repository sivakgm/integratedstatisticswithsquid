/*
 * RowDataDenied.cpp
 *
 *  Created on: Nov 18, 2014
 *      Author: sivaprakash
 */

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

void insertDenObjIntoTable(int pointObj,DBConnection *statLog)
{
	try
	{
		if(rowDataDen[pointObj]->isInTable == 1)
		{
			updateTableDen(rowDataDen[pointObj],statLog->stmt,currentTableDen);
		}
		else
		{
			insertIntoTableDen(rowDataDen[pointObj],statLog->stmt,currentTableDen);
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

		insertIntoTableDenTime(rowdataden,log->tim,statLog->stmt,currentTableDen);
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
void insertAllDenObjDataIntoTable(DBConnection *statLog)
{
	try
	{
		for(int i=0;i<NoDENOBJ;i++)
		{
			insertDenObjIntoTable(i,statLog);
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
