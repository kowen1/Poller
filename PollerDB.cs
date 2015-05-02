using System;
using System.Configuration;
using System.Data;
using System.Data.Odbc;

namespace Poller
{
	/// <summary>
	/// Summary description for PollerDB.
	/// </summary>
	public class PollerDB : IDisposable
	{
		private OdbcConnection	odbcConn = null;

		public OdbcConnection Connection
		{
			get
			{
				return odbcConn ;
			}
		}
		
		public void Open()
		{
			try
			{
				odbcConn = new OdbcConnection();
				odbcConn.ConnectionString = PollerConfig.m_sConnStr;
				odbcConn.Open();
			}
			catch (Exception e)
			{
				Logger.LogString("Error opening database with odbc connection string: " + PollerConfig.m_sConnStr);
				Logger.LogString("Error message is: " + e.Message );
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown Exception opening database with odbc connection string: " + PollerConfig.m_sConnStr);
				throw;
			}
		}

		public void Close()
		{
			if (odbcConn != null)
			{
				if (odbcConn.State != ConnectionState.Closed)
				{
					try
					{
						odbcConn.Close();
						odbcConn.Dispose();
					}
					catch (Exception)
					{
						throw;
					}
					finally
					{
						odbcConn = null ;
					}
				}
			}
		}

		public void Dispose()
		{
			try
			{
				// close application DB
				Close();
			}
			finally
			{
				// clean up class objects

			}
			return;
		}


		public PollerDB()
		{
			this.Open();
		}

		~PollerDB()
		{
			Dispose() ;
		}

	}
}
