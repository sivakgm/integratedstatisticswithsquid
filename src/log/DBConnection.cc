/*
 * DBConnection.cpp

 *
 *  Created on: Nov 5, 2014
 *      Author: sivaprakash
 *
 */
#include "squid.h"
#include "log/DBConnection.h"
#include "log/RowDataDenied.h"
#include "log/RowData.h"

void DBConnection::createDBIfNotExists(string schema)
{
	try
	{
		this->stmt=this->conn->createStatement();
		this->stmt->execute("create database if not exists "+schema+";");
		return;
	}
	catch (sql::SQLException &e)
	{
		syslog(LOG_NOTICE,e.what());
        	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	        syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
	}
}

void DBConnection::createStatTable(int flag,string tableNam)
{
	try
	{
	/////////////////////////////////////////////////////
		this->stmt=this->conn->createStatement();
		if(tableNam != "")
		{
			if(flag == 1)
			{
				this->tableNameYearAcc = "ud_acc_"+tableNam;
				this->tableNameYearDen = "ud_den_"+tableNam;
				this->stmt->execute("create table if not exists ud_acc_" + tableNam + " (user varchar(16), domain varchar(100), size double, connection double, hit double, miss double,response_time double);");
				this->stmt->execute("create table if not exists ud_den_" + tableNam + " (user varchar(16), domain varchar(100),connection double);");
			}
			else
			{
				this->tableNameMonthAcc = "ud_acc_"+tableNam;
				this->tableNameMonthDen = "ud_den_"+tableNam;
				this->stmt->execute("create table if not exists ud_acc_" + tableNam + " (user varchar(16), domain varchar(100), size double, connection double, hit double, miss double,response_time double);");
				this->stmt->execute("create table if not exists ud_den_" + tableNam + " (user varchar(16), domain varchar(100),connection double);");
			}
		}
	}
	catch (sql::SQLException &e)
	{
		syslog(LOG_NOTICE,e.what());
	        syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	        syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
	}
}

void DBConnection::dbConnOpen(string host,string port,string user,string pass,string schema)
{
	try
	{
		Driver *driver = get_driver_instance();
		string addre = "tcp://"+host+":"+port;
		this->conn = driver->connect(addre,user,pass);
		this->createDBIfNotExists(schema);
		this->conn->setSchema(schema);
		return;
	}
	catch (sql::SQLException &e)
	{
		syslog(LOG_NOTICE,e.what());
        	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	        syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
	}
	catch (exception& e)
	{	
		syslog(LOG_NOTICE,e.what());
        	syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
	        syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
	}
}

void DBConnection::createStatTableName(string tableNam)
{

	try
	{
		this->tableNameAcc = "ud_acc_"+tableNam;
		this->tableNameDen = "ud_den_"+tableNam;
		this->tableNameAccTime = "at_acc_" + tableNam;
		this->tableNameDenTime= "at_den_" + tableNam;
		this->createTableIfNotExist();
//		this->setPstmt();
		return;
	}
	catch (sql::SQLException &e)
	{
                syslog(LOG_NOTICE,e.what());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
	}
}

void DBConnection::createTableIfNotExist()
{
	try
	{
		this->stmt=this->conn->createStatement();
		this->stmt->execute("create table if not exists " + this->tableNameAcc + " (user varchar(16), domain varchar(100), size double, connection double, hit double, miss double,response_time double);");
		this->stmt->execute("create table if not exists " + this->tableNameDen + " (user varchar(16), domain varchar(100), connection double);");
		this->stmt->execute("create table if not exists " + this->tableNameAccTime + " (user varchar(16), domain varchar(100), accTime varchar(15));");
		this->stmt->execute("create table if not exists " + this->tableNameDenTime + " (user varchar(16), domain varchar(100), accTime varchar(15));");
	}
	catch (sql::SQLException &e)
	{
	        syslog(LOG_NOTICE,e.what());
	        syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());

	}
}

/*void DBConnection::setPstmt()
{
	try
	{
		string query = "insert into " + this->tableNameAcc +"(user,domain,size,connection,hit,miss,response_time) values(?,?,?,?,?,?,?)";
		this->insPstmtAcc=this->conn->prepareStatement(query);

		query = "update " + this->tableNameAcc + " set size=?,connection=?,hit=?,miss=?,response_time=? where user=? and domain=?;";
		this->upPstmtAcc=this->conn->prepareStatement(query);

		query = "insert into " + this->tableNameDen +"(user,domain,connection) values(?,?,?)";
		this->insPstmtDen=this->conn->prepareStatement(query);

		query = "update " + this->tableNameDen + " set connection=? where user=? and domain=?;";
		this->upPstmtDen=this->conn->prepareStatement(query);

		query = "insert into " + this->tableNameAccTime +"(user,domain,accTime) values(?,?,?);";
		this->insPstmtAccTime=this->conn->prepareStatement(query);

		query = "insert into " + this->tableNameDenTime +"(user,domain,accTime) values(?,?,?);";
		this->insPstmtDenTime=this->conn->prepareStatement(query);
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
}


void DBConnection::setReadPstmt(int flag,string tableNam,string user,string domain)
{
	try
	{
		//syslog(LOG_NOTICE,"DB:set read pstmt");
		string query;

		if(flag == 1)
		{
			query = "select * from " + tableNam +";";
		}
		else
		{
			query = "select * from " + tableNam +" where user=? and domain=?;";
		}

		this->readpstmt = this->conn->prepareStatement(query);

		if(flag != 1 )
		{
			this->readpstmt->setString(1,user);
			this->readpstmt->setString(2,domain);
		}
	}
	catch (sql::SQLException &e)
	{
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
	  	cout << "# ERR: " << e.what();
	  	cout << " (MySQL error code: " << e.getErrorCode();
	  	cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	}
	//syslog(LOG_NOTICE,"DB:end set read pstmt");
}*/


/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

string timeAndDate()
{
	try
	{
		time_t now = time(0);
		tm *ltm = localtime(&now);
		string date = boost::lexical_cast<std::string>((ltm->tm_mday < 10 ?"0":""))+boost::lexical_cast<std::string>(ltm->tm_mday)+"_"+boost::lexical_cast<std::string>(1 + ltm->tm_mon < 10 ?"0":"")+boost::lexical_cast<std::string>(1 + ltm->tm_mon)+"_"+boost::lexical_cast<std::string>(1900 + ltm->tm_year);
		return date;
	}
	catch (exception& e)
	{
	        syslog(LOG_NOTICE,e.what());
	        syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
	}
	return NULL;
}

void insertIntoTableAccTime(RowData *rowData,string acctime,Statement *stmt,string tn)
{
	try
	{
		stmt->execute("insert into " + tn + "(user,domain,accTime) values('"+rowData->user+"','" + rowData->domain + "','"+ acctime +"');");
	}
	catch (sql::SQLException &e)
	{
                syslog(LOG_NOTICE,e.what());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());

	}
}

void insertIntoTableDenTime(RowDataDenied *rowDataDen,string acctime,Statement *stmt,string tn)
{
	try
	{
		stmt->execute("insert into " + tn + "(user,domain,accTime) values('"+rowDataDen->user+"','" + rowDataDen->domain + "','"+ acctime +"');");
	}
	catch (sql::SQLException &e)
	{
                syslog(LOG_NOTICE,e.what());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
	}
}


void insertIntoTableAcc(RowData *rowData,Statement *stmt,string tn)
{
	try
	{
		syslog(LOG_NOTICE,"DB:: insert data");
		stmt->execute("insert into " + tn +"(user,domain,size,connection,hit,miss,response_time) values('"+rowData->user+"','"+rowData->domain+"','"+boost::lexical_cast<std::string>(rowData->size)+"','"+ boost::lexical_cast<std::string>(rowData->connection)+"','"+boost::lexical_cast<std::string>(rowData->hit)+"','"+boost::lexical_cast<std::string>(rowData->miss)+"','"+boost::lexical_cast<std::string>(rowData->response_time)+"');");
		syslog(LOG_NOTICE,"DB:: insert data");
	}
	catch (sql::SQLException &e)
	{
                syslog(LOG_NOTICE,e.what());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
	}
}

void updateTableAcc(RowData *rowData,Statement *stmt,string tn)
{
	try
	{
		if(tn.substr(0,4) == "swap")
                {
			syslog(LOG_NOTICE,"DB:: update data");
			stmt->execute("update " + tn + " set size='"+boost::lexical_cast<std::string>(rowData->size)+"',connection='"+boost::lexical_cast<std::string>(rowData->connection)+"',hit='"+boost::lexical_cast<std::string>(rowData->hit)+"',miss='"+boost::lexical_cast<std::string>(rowData->miss)+"',response_time='"+boost::lexical_cast<std::string>(rowData->response_time)+"',isInObj=0 where user='"+rowData->user+"' and domain='"+rowData->domain+"';");	
			syslog(LOG_NOTICE,"DB:: update data");
		}
		else
		{
			syslog(LOG_NOTICE,"DB:: update data no isinobj");
                        stmt->execute("update " + tn + " set size='"+boost::lexical_cast<std::string>(rowData->size)+"',connection='"+boost::lexical_cast<std::string>(rowData->connection)+"',hit='"+boost::lexical_cast<std::string>(rowData->hit)+"',miss='"+boost::lexical_cast<std::string>(rowData->miss)+"',response_time='"+boost::lexical_cast<std::string>(rowData->response_time)+"' where user='"+rowData->user+"' and domain='"+rowData->domain+"';");
                        syslog(LOG_NOTICE,"DB:: update data isinobj");
		}
		return;
	}
	catch (sql::SQLException &e)
	{
                syslog(LOG_NOTICE,e.what());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
	}
}

void updateTableIsInObj(Statement *stmt,string tn,string user,string domain)
{
        try
        {
                syslog(LOG_NOTICE,"DB:: update data is in obj");
                stmt->execute("update " + tn + " set isInObj=1 where user='"+user+"' and domain='"+domain+"';");
                syslog(LOG_NOTICE,"DB:: update datais in obj");
                return;
        }
        catch (sql::SQLException &e)
        {
                syslog(LOG_NOTICE,e.what());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
        }
}



void insertIntoTableDen(RowDataDenied *rowData,Statement *stmt,string tn)
{
	try
	{
	syslog(LOG_NOTICE,"DB:: denied insert data");
	stmt->execute("insert into " + tn +"(user,domain,connection) values('"+rowData->user+"','"+rowData->domain+"','"+boost::lexical_cast<std::string>(rowData->connection)+"');");
	syslog(LOG_NOTICE,"DB:: denied insert data");
	return;
	}
	catch (sql::SQLException &e)
	{
                syslog(LOG_NOTICE,e.what());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
	}
}

void updateTableDen(RowDataDenied *rowData,Statement *stmt,string tn)
{
	try
	{
		if(tn.substr(0,4) == "swap")
		{
			syslog(LOG_NOTICE,"DB:: denied update data");
			stmt->execute("update " + tn + " set connection='"+boost::lexical_cast<std::string>(rowData->connection)+"',isInObj=0 where user='"+rowData->user+"' and domain='"+rowData->domain+"';");
			syslog(LOG_NOTICE,"DB:: denied update data");
		}
		else
		{
			syslog(LOG_NOTICE,"DB:: denied update data");
                        stmt->execute("update " + tn + " set connection='"+boost::lexical_cast<std::string>(rowData->connection)+"',isInObj=0 where user='"+rowData->user+"' and domain='"+rowData->domain+"';");
                        syslog(LOG_NOTICE,"DB:: denied update data");
		}
		return;
	}
	catch (sql::SQLException &e)
	{
                syslog(LOG_NOTICE,e.what());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
	}
}

void DBConnection::readTable(int flag,string tableNam,string user,string domain)
{
	try
	{	
		string query;

                if(flag == 1)
                {
                        query = "select * from " + tableNam +";";
                }
                else
                {
                        query = "select * from " + tableNam +" where user='"+user+"' and domain='"+domain+"';";
                }

		this->res = this->stmt->executeQuery(query);
		
		return;
	}
	catch (sql::SQLException &e)
	{
                syslog(LOG_NOTICE,e.what());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());
	}
}

string parseURLtoDomain(string url)
{
	try
	{
		string delimiters = "/";
		size_t current = 0;
		size_t next = 0;
		string domain = url;

		next = url.find_first_of( delimiters, current );

		if(next != string::npos)
		{
			if( url[next + 1] == '/')
			{
				current = next + 2;
				next = url.find_first_of( delimiters, current );
				domain = url.substr( current, next - current );
			}
			else
			{
				domain = url.substr( current, next );
			}
		}

		return domain;
	}
	catch (exception& e)
	{
		syslog(LOG_NOTICE,e.what());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__FILE__).c_str());
                syslog(LOG_NOTICE,boost::lexical_cast<std::string>(__LINE__).c_str());

	}
	return NULL;
}
