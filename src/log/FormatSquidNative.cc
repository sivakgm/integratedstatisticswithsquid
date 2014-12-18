/*
 * DEBUG: section 46    Access Log - Squid format
 * AUTHOR: Duane Wessels
 *
 * SQUID Web Proxy Cache          http://www.squid-cache.org/
 * ----------------------------------------------------------
 *
 *  Squid is the result of efforts by numerous individuals from
 *  the Internet community; see the CONTRIBUTORS file for full
 *  details.   Many organizations have provided support for Squid's
 *  development; see the SPONSORS file for full details.  Squid is
 *  Copyrighted (C) 2001 by the Regents of the University of
 *  California; see the COPYRIGHT file for full details.  Squid
 *  incorporates software developed and/or copyrighted by other
 *  sources; see the CREDITS file for full details.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111, USA.
 *
 */

#include "squid.h"
#include "AccessLogEntry.h"
#include "format/Quoting.h"
#include "format/Token.h"
#include "globals.h"
#include "HttpRequest.h"
#include "log/File.h"
#include "log/Formats.h"
#include "SquidConfig.h"
#include "SquidTime.h"

//Modified to insert log into DB

#include <ctime>
#include<sys/time.h>
#include <string>
#include <mysql_connection.h>
#include <mysql_driver.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <boost/lexical_cast.hpp>




//#################################

#include <thread>
#include <fstream>
#include <cstdlib>
#include <time.h>

#include "log/DBConnection.h"
#include "log/RowData.h"
#include "log/RowDataDenied.h"
#include "log/grossStatistics.h"
#include "log/DomainStatistics.h"
#include "log/UserStatistics.h"

//###################################



extern sql::Driver *driver;
extern sql::Connection *conn;
extern sql::PreparedStatement *pstmt;


//#############################




// ########################## Sending Squid Log to generate the statistics




using namespace std;

static int startFlag = 1;
DBConnection *statLog;

string previousLogDate = "";
string processDateFromConfFile = "";
string previousLogYear="",previousLogMonth="",previousLogDay="";


const int MAXDENIEDOBJ = 4;
int NoDENOBJ;

const int MAXACCESSOBJ = 4;
int NoACCOBJ;

RowDataDenied *rowDataDen[MAXDENIEDOBJ];
RowData *rowDataAcc[MAXACCESSOBJ];

// ##################################




void
Log::Format::SquidNative(const AccessLogEntry::Pointer &al, Logfile * logfile)
{
    char hierHost[MAX_IPSTRLEN];

    const char *user = NULL;


    //#############
    string currentLogTime,currentLogDate;
    //##########



	//Modified for DB log
	
	tm *ltm = localtime(&current_time.tv_sec);

	//########################

#if USE_AUTH
    if (al->request && al->request->auth_user_request != NULL)
        user = ::Format::QuoteUrlEncodeUsername(al->request->auth_user_request->username());
#endif

    if (!user)
        user = ::Format::QuoteUrlEncodeUsername(al->cache.extuser);

#if USE_SSL
    if (!user)
        user = ::Format::QuoteUrlEncodeUsername(al->cache.ssluser);
#endif

    if (!user)
        user = ::Format::QuoteUrlEncodeUsername(al->cache.rfc931);

    if (user && !*user)
        safe_free(user);

    char clientip[MAX_IPSTRLEN];
    al->getLogClientIp(clientip, MAX_IPSTRLEN);

    logfilePrintf(logfile, "%9ld.%03d %6d %s %s%s/%03d %" PRId64 " %s %s %s %s%s/%s %s%s",
                  (long int) current_time.tv_sec,
                  (int) current_time.tv_usec / 1000,
                  al->cache.msec,
                  clientip,
                  LogTags_str[al->cache.code],
                  al->http.statusSfx(),
                  al->http.code,
                  al->cache.replySize,
                  al->_private.method_str,
                  al->url,
                  user ? user : dash_str,
                  al->hier.ping.timedout ? "TIMEOUT_" : "",
                  hier_code_str[al->hier.code],
                  al->hier.tcpServer != NULL ? al->hier.tcpServer->remote.toStr(hierHost, sizeof(hierHost)) : "-",
                  al->http.content_type,
                  (Config.onoff.log_mime_hdrs?"":"\n"));

    safe_free(user);

//Log insertion DB insertion
 //**********************************************************************************

	try
	{

        	try
        	{
			 pstmt->setString(1,boost::lexical_cast<std::string>(current_time.tv_sec));

		//inserting date	
		//	currentLogDate = boost::lexical_cast<std::string>(1900 + ltm->tm_year)+"-"+boost::lexical_cast<std::string>(1 + ltm->tm_mon < 10 ?"0":"")+boost::lexical_cast<std::string>(1 + ltm->tm_mon)+"-"+boost::lexical_cast<std::string>((ltm->tm_mday < 10 ?"0":""))+boost::lexical_cast<std::string>(ltm->tm_mday);
//			currentLogDate = boost::lexical_cast<std::string>(1900 + ltm->tm_year)+"-"+boost::lexical_cast<std::string>(1 + ltm->tm_mon < 10 ?"0":"")+boost::lexical_cast<std::string>(1 + ltm->tm_mon)+"-"+boost::lexical_cast<std::string>((ltm->tm_mday < 10 ?"0":""))+boost::lexical_cast<std::string>(ltm->tm_mday);
			currentLogDate = boost::lexical_cast<std::string>((ltm->tm_mday < 10 ?"0":""))+boost::lexical_cast<std::string>(ltm->tm_mday) + "-"+boost::lexical_cast<std::string>(1 + ltm->tm_mon < 10 ?"0":"")+boost::lexical_cast<std::string>(1 + ltm->tm_mon)+"-"+boost::lexical_cast<std::string>(1900 + ltm->tm_year);
			pstmt->setString(2,boost::lexical_cast<std::string>(1900 + ltm->tm_year)+"-"+boost::lexical_cast<std::string>(1 + ltm->tm_mon < 10 ?"0":"")+boost::lexical_cast<std::string>(1 + ltm->tm_mon)+"-"+boost::lexical_cast<std::string>((ltm->tm_mday < 10 ?"0":""))+boost::lexical_cast<std::string>(ltm->tm_mday));

		//inserting time
			currentLogTime = boost::lexical_cast<std::string>(ltm->tm_hour)+":"+boost::lexical_cast<std::string>((ltm->tm_min < 10 ?"0":"")) +boost::lexical_cast<std::string>( ltm->tm_min) + ":" + boost::lexical_cast<std::string>(( ltm->tm_sec < 10 ?"0":""))+boost::lexical_cast<std::string>( ltm->tm_sec);
			pstmt->setString(3,boost::lexical_cast<std::string>(ltm->tm_hour)+":"+boost::lexical_cast<std::string>((ltm->tm_min < 10 ?"0":"")) +boost::lexical_cast<std::string>( ltm->tm_min) + ":" + boost::lexical_cast<std::string>(( ltm->tm_sec < 10 ?"0":""))+boost::lexical_cast<std::string>( ltm->tm_sec));
		
			pstmt->setInt(4,al->cache.msec);
    	    		pstmt->setString(5,clientip);
	        	pstmt->setString(6,LogTags_str[al->cache.code]);
    			pstmt->setString(7,boost::lexical_cast<std::string>(al->http.code));
	        	pstmt->setInt(8,al->cache.replySize);
    			pstmt->setString(9,al->_private.method_str);
        		pstmt->setString(10,al->url);
	        	pstmt->setString(11,user ? user : dash_str);
    			pstmt->setString(12,hier_code_str[al->hier.code]);
        		pstmt->setString(13,al->hier.tcpServer != NULL ? al->hier.tcpServer->remote.toStr(hierHost, sizeof(hierHost)) : "-");
	        	pstmt->setString(14,al->http.content_type);

    			pstmt->executeUpdate();
        	}
        	catch(sql::SQLException &e)
        	{
			syslog(LOG_NOTICE,"Re-trying to connect MySql DB");
			driver = get_driver_instance();
	        	conn = driver->connect("tcp://127.0.0.1:3306","root","simple");
    	    		conn->setSchema("squid");
        		pstmt = conn->prepareStatement("insert into access_log(time_since_epoch,date_day,date_time,response_time,client_src_ip_addr,squid_request_status,http_status_code,reply_size,request_method,request_url,username,squid_hier_status,server_ip_addr,mime_type) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
			syslog(LOG_NOTICE,"Connected to MySQL");
	 	}	
	}
	catch(sql::SQLException &e)
	{
		syslog(LOG_NOTICE,"MySql Connection Failed");
	}

//********************************************************************************************


	//######################## Sending Squid log to generate the statistics##############################

	if(startFlag == 1)
	{ 
		syslog(LOG_NOTICE,"Nat::First time code execution and db connection establishment");
		statLog = new DBConnection();
	}

	string userIp,domain,dateForTN;
	int pointObj,isnewLogInTable;

	userIp = clientip;
	domain = parseURLtoDomain(al->url);

	if(currentLogDate != previousLogDate)
	{
		previousLogDate = currentLogDate;
		dateForTN = currentLogDate;

		//Replacing '-' with '_' Ex: 28-12-2014 = 28_12_2014
		for(unsigned int x=0;x<dateForTN.length();x++)
		{
			if(dateForTN[x] == '-')
			{
				dateForTN[x]='_';
			}
		}

		//Checking whether lastly processed date(which is stored in separate configuration file) is same as current date
	/*	if((processDateFromConfFile != currentLogDate && processDateFromConfFile != "a") && startFlag == 1)
		{
			syslog(LOG_NOTICE,"inside configuration file stat analysis");
			string temTN = "ud_acc_"+processDateFromConfFile;
			thread t1(grossStatisticsAcc,temTN);
			//t1.join();

			temTN = "ud_den_"+processDateFromConfFile;
			thread t2(grossStatisticsDen,temTN);
			//t2.join();
		}*/

		//checking whether previous log year is same as current log year
		//           1. If year changes, then new db is created
		//			 2. For the time, the connection to the database is opened
		if(previousLogYear != currentLogDate.substr(6,4))
		{
			previousLogYear=currentLogDate.substr(6,4);
			string dbName = "squidStatistics_"+previousLogYear;
			statLog->dbConnOpen("127.0.0.1","3306","root","simple",dbName);
			statLog->createStatTable(1,previousLogYear);
		}


		if(previousLogMonth != currentLogDate.substr(3,2))
		{
			previousLogMonth = currentLogDate.substr(3,2);
			statLog->createStatTable(0,previousLogMonth);
		}

		if(previousLogDay != currentLogDate.substr(0,2))
		{
			if(previousLogDay != "")
			{
				string temTN = statLog->tableNameAcc;

				insertAllObjDataIntoTable(statLog);
				thread t1(grossStatisticsAcc,temTN);
				//t1.join();

				insertAllDenObjDataIntoTable(statLog);
				temTN = statLog->tableNameDen;
				thread t2(grossStatisticsDen,temTN);
				//t2.join();

			}
			previousLogDay = currentLogDate.substr(0,2);
		}
		statLog->createStatTableName(dateForTN);
		syslog(LOG_NOTICE,"NAT::End of db connection code");

	}
	startFlag = 0;

	/////////////////////////////////////////////////////////
	string sta = "TCP_DENIED";

	if( LogTags_str[al->cache.code] != sta.c_str())
	{
		syslog(LOG_NOTICE,"start to PPPPPPPPPPParse log");
		logDataAcc *dataLog = new logDataAcc();
		syslog(LOG_NOTICE,"created obj for the structure");
		dataLog->domain = domain;
		dataLog->response_time = al->cache.msec;
		dataLog->size = al->cache.replySize;
		dataLog->status = LogTags_str[al->cache.code];
		dataLog->tim = currentLogTime;
		dataLog->user = userIp;

					syslog(LOG_NOTICE,"check data in object");

					pointObj = checkDataInOBJ(NoACCOBJ,userIp,domain);

					if(pointObj != -1)
					{
						syslog(LOG_NOTICE,"update data in object 1");
						updateDataInObj(statLog,rowDataAcc[pointObj],dataLog);
					}
					else
					{
						if(NoACCOBJ<MAXACCESSOBJ)
						{
							syslog(LOG_NOTICE,"create new object");
							createNewObj();
							pointObj = NoACCOBJ -1;
						}
						else
						{
							syslog(LOG_NOTICE,"insert object into table");
							pointObj = getLeastObjPriority();
							insertObjIntoTable(pointObj,statLog);
							emptyTheObj(pointObj);
						}

						syslog(LOG_NOTICE,"check data in table");
						isnewLogInTable = checkDataInTable(statLog,statLog->tableNameAcc,userIp,domain);

						if(isnewLogInTable == 1)
						{	
							syslog(LOG_NOTICE,"updateobjfromtable");
							updateObjFromTable(pointObj,statLog->res);
							updateDataInObj(statLog,rowDataAcc[pointObj],dataLog);
						}
						else
						{
							updateDataInObj(statLog,rowDataAcc[pointObj],dataLog);
						}
					}
					syslog(LOG_NOTICE,"end of parse log");
				}
				else
				{
					logDataDen *dataLog = new logDataDen();
					dataLog->domain = domain;
					dataLog->tim = currentLogTime;
					dataLog->user = userIp;


					pointObj = checkDataInDenOBJ(NoDENOBJ,userIp,domain);

					if(pointObj != -1)
					{
						updateDataInDenObj(statLog,rowDataDen[pointObj],dataLog);
					}
					else
					{
						if(NoDENOBJ<MAXDENIEDOBJ)
						{
							createNewDenObj();
							pointObj = NoDENOBJ -1;
						}
						else
						{
							pointObj = getLeastDenObjPriority();
							insertDenObjIntoTable(pointObj,statLog);
							emptyTheDenObj(pointObj);
						}

						isnewLogInTable = checkDataInTable(statLog,statLog->tableNameDen,userIp,domain);

						if(isnewLogInTable == 1)
						{
							updateDenObjFromTable(pointObj,statLog->res);
							updateDataInDenObj(statLog,rowDataDen[pointObj],dataLog);
						}
						else
						{
							updateDataInDenObj(statLog,rowDataDen[pointObj],dataLog);
						}
					}
				}

	processDateFromConfFile = dateForTN;



	////////////////////////////////////////////////////////



	//****************** EOD -Sending Squid log to generate the statistics **********************************************


    if (Config.onoff.log_mime_hdrs) {
        char *ereq = ::Format::QuoteMimeBlob(al->headers.request);
        char *erep = ::Format::QuoteMimeBlob(al->headers.reply);
        logfilePrintf(logfile, " [%s] [%s]\n", ereq, erep);
        safe_free(ereq);
        safe_free(erep);
    }
}
