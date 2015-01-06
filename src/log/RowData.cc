/*
 * RowData.cpp
 *
 *  Created on: Nov 5, 2014
 *      Author: sivaprakash
 */
#include "squid.h"
#include "log/RowData.h"
#include "log/DBConnection.h"

extern const int MAXACCESSOBJ = 4;
extern int NoACCOBJ;
extern string currentTableAcc;

extern RowData *rowDataAcc[MAXACCESSOBJ];

RowData::RowData(void)
{
	user = "";
	domain = "";
	connection = 0;
	miss = 0.0;
	hit = 0.0;
	size = 0.0;
	response_time = 0.0;
	priority = 0;
	isInTable = 0;
}

/*RowData::~RowData() {
	// TODO Auto-generated destructor stub
}*/

void insertAllObjDataIntoTable(DBConnection *statLog)
{
	try
	{
		for(int i=0;i<NoACCOBJ;i++)
		{
			insertObjIntoTable(i,statLog);
		}
		for(int i=0;i<NoACCOBJ;i++)
		{
			delete rowDataAcc[i];
		}
		NoACCOBJ = 0;
	}
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}
}

void setObjPriority(int lim)
{
	try
	{
		if(lim == 0)
		{
			lim = NoACCOBJ;
		}
/*		for(int i=0;i<lim;i++)
		{
			rowDataAcc[i]->priority = rowDataAcc[i]->priority + 1;
		}*/
		for(int i=0;i<NoACCOBJ;i++)
		{
			if( rowDataAcc[i]->priority < lim )
			{
				rowDataAcc[i]->priority = rowDataAcc[i]->priority + 1;
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

int getLeastObjPriority()
{
	try
	{
		for(int i=0;i<NoACCOBJ;i++)
		{
			if(rowDataAcc[i]->priority == NoACCOBJ )
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

void createNewObj()
{
	try
	{
		rowDataAcc[NoACCOBJ] = new RowData();
		NoACCOBJ++;
	}
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}
	return;
}

void emptyTheObj(int pointObj)
{
	try
	{
		rowDataAcc[pointObj]->user = "";
		rowDataAcc[pointObj]->domain = "";
		rowDataAcc[pointObj]->connection = 0;
		rowDataAcc[pointObj]->miss = 0.0;
		rowDataAcc[pointObj]->hit = 0.0;
		rowDataAcc[pointObj]->size = 0.0;
		rowDataAcc[pointObj]->isInTable = 0;
		rowDataAcc[pointObj]->response_time = 0;
	}
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}
}

void updateObjFromTable(int pointObj,ResultSet *res)
{
	try
	{
		rowDataAcc[pointObj]->user = res->getString(1);
		rowDataAcc[pointObj]->domain = res->getString(2);
		rowDataAcc[pointObj]->connection = res->getInt(4);
		rowDataAcc[pointObj]->miss = res->getDouble(6);
		rowDataAcc[pointObj]->hit =  res->getDouble(5);
		rowDataAcc[pointObj]->size = res->getDouble(3);
		rowDataAcc[pointObj]->response_time = res->getDouble(7);
		rowDataAcc[pointObj]->isInTable = 1;
	}
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}
}

void insertObjIntoTable(int pointObj,DBConnection *statLog)
{
	try
	{
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		if(rowDataAcc[pointObj]->isInTable == 1)
		{	
			//syslog(LOG_NOTICE,"RD:: update data");
			updateTableAcc(rowDataAcc[pointObj],statLog->stmt,currentTableAcc);
			//syslog(LOG_NOTICE,"RD:: End of update data");
		}
		else
		{	
//			syslog(LOG_NOTICE,"RD:: Insert data");
			insertIntoTableAcc(rowDataAcc[pointObj],statLog->stmt,currentTableAcc);
//			syslog(LOG_NOTICE,"RD:: End of Insert data");
		}
	}
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}
}

void updateDataInObj(DBConnection *statLog,RowData *rowdata,logDataAcc *log)
{
	try
	{
		int lim = rowdata->priority;
		rowdata->user = log->user;
		rowdata->domain = log->domain;
		rowdata->connection = rowdata->connection + 1;
		rowdata->size = rowdata->size + log->size;
		rowdata->response_time = rowdata->response_time + log->response_time;
		rowdata->priority = 0;
		if(log->status == "TCP_HIT" ||log->status == "TCP_MEM_HIT" || log->status == "UDP_HIT" || log->status == "UDP_HIT_OBJ")
		{
			rowdata->hit = rowdata->hit + log->size;
		}
		else
		{
			rowdata->miss = rowdata->miss + log->size;
		}
		insertIntoTableAccTime(rowdata,log->tim,statLog->stmt,statLog->tableNameAccTime);
		setObjPriority(lim);
		return;
	}
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}
}


int checkDataInOBJ(int count,string user,string domain)
{
	try
	{
		for(int i=0;i<count;i++)
		{
			if(rowDataAcc[i]->user == user && rowDataAcc[i]->domain == domain)
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


/*void readResSet(DBConnection *logDB)
{
	while(logDB->res->next())
	{
		cout<<"\t"<<logDB->res->getString(1);
		cout<<"\t"<<logDB->res->getString(2);
		cout<<"\t"<<logDB->res->getDouble(3);
		cout<<"\t"<<logDB->res->getInt(4);
		cout<<"\t"<<logDB->res->getDouble(5);
		cout<<"\t"<<logDB->res->getDouble(6)<<"\n";

		cout<<"\t"<<logDB->res->getString(1);
		cout<<"\t"<<logDB->res->getString(2);
		cout<<"\t"<<logDB->res->getInt(3)<<endl;

	}
	return;
}*/


int checkDataInTable(DBConnection *statLog,string tableName,string user,string domain)
{
	try
	{
//		syslog(LOG_NOTICE,"start in row data check table");
//		statLog->setReadPstmt(0,tableName,user,domain);
//		syslog(LOG_NOTICE,"calling read table");
		statLog->readTable(0,tableName,user,domain);
		if(statLog->res->next())
		{
			return 1;
		}
		
	}
	catch (exception& e)
	{
		cout << "# ERR File: " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	    cout << e.what() << '\n';
	}
//	syslog(LOG_NOTICE,"end in row data check table");
	return -1;
}
