using System;
using System.Data;
using System.Data.Odbc;
using System.IO;
using System.Configuration;

namespace Poller
{
	/// <summary>
	/// 
	/// </summary>
    public class FaultData
	{
		// zzzz_ddmmyy_hhmmss.eee
		// Where
		//   zzzz = Fault Type 
		//   hh  = time of call - hour
		//   mm  = time of call -  min
		//   ss = time of call - sec
		//   dd = time of call - day
		//   mm = time of call -  month
		//   yy = time of call - year
		//   eee extension = Flt,Acc,Clr
	
		enum updateType { InsertNewItem, ReplaceExisting, KeepExisting, Error, ExactDup };

		private string m_strFaultDescription;
		private string m_strFaultLocation;	// Fault type - up to 40 chars
		private string m_strFilePath;		// Full path and file name
		private string m_strFileName;		// Just the file name
		private string m_strFaultStatus;  // can be one of {reported, accepted, cleared}
		private System.DateTime m_dateTimeCall;

		public FaultData(string p_strFilePath)
		{
			Process(p_strFilePath);
		}

		public static void PurgeFaultData(DateTime deleteBefore)
		{
			try
			{
				PollerDB db = new PollerDB();

				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText = "DELETE FROM FaultLog WHERE " +
					"FaultDateTime < #" + deleteBefore.ToString("yyyy/MM/dd 00:00:00") + "#";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				int Result = cmd.ExecuteNonQuery();
				Logger.LogInfoString(Result + " row(s) purged from FaultLog table");
				cmd.Dispose();
				db.Close();
			}
			catch ( Exception e) 
			{
				Logger.LogString("Error purging from FaultLog table");
				Logger.LogString("Error message is: " + e.Message );
			}
			catch 
			{
				Logger.LogString("Unknown exception purging from FaultLog table");
			}
		}

		public void ReadFile()
		{
			try
			{
				if (File.Exists(this.m_strFilePath) )
				{
					StreamReader sr = File.OpenText(this.m_strFilePath);
					this.m_strFaultDescription = sr.ReadToEnd();
					char[] trimarray = {'\n', '\r'};
					this.m_strFaultDescription = this.m_strFaultDescription.TrimEnd(trimarray);
					sr.Close();
					if (this.m_strFaultDescription.Length > 255)
					{
						Logger.LogString("Description > 255 bytes, truncating to 255 bytes");
						this.m_strFaultDescription = this.m_strFaultDescription.Substring(0, 255);
					}
				}
				else
				{
					Exception e = new Exception("Error reading file " + this.m_strFileName + ",  file does not exist");
					throw e;
				}
			}
			catch (Exception e)
			{
				Logger.LogString("Error Reading file " + this.m_strFilePath);
				Logger.LogString("Error message is: " + e.Message );
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown exception Reading file " + this.m_strFileName);
				throw;
			}
		}

		private void parseFileName(string p_strFilePath)
		{
			try
			{
				// Save a copy of the full file path + file name
				this.m_strFilePath = p_strFilePath;

				// Save file name only
				this.m_strFileName = Path.GetFileName(this.m_strFilePath);

				string strExtension = Path.GetExtension(this.m_strFilePath).ToLower();

				if ( strExtension == ".acc" )
					this.m_strFaultStatus = "accepted";
				else if ( strExtension == ".flt" )
					this.m_strFaultStatus = "reported";
				else if ( strExtension == ".clr" )
					this.m_strFaultStatus = "cleared";
				else
				{
					System.Exception e = new System.Exception("Invalid Filename: fault status: " + strExtension + " not recognised, {.acc, .flt, .clr} expected");
					throw e;
				}

				// get fault type
				int index = this.m_strFileName.IndexOf("_");
				if (index == -1)
				{
					System.Exception e = new System.Exception("Invalid Filename: fault type not found");
					throw e;
				}
				if (index >= 40)
				{
					System.Exception e = new System.Exception("Invalid Filename: fault type > 40 chars");
					throw e;
				}

				this.m_strFaultLocation = this.m_strFileName.Substring(0, index);

				// get call datetime
				index += 1;
				System.Globalization.CultureInfo info =
					new System.Globalization.CultureInfo("en-GB", false);

				System.Globalization.Calendar calendar = info.Calendar;

				// Format is _ddmmyy_hhmmss
				this.m_dateTimeCall = new System.DateTime(
					Convert.ToInt32(this.m_strFileName.Substring(index+4, 2)) + 2000,
					Convert.ToInt32(this.m_strFileName.Substring(index+2, 2)),
					Convert.ToInt32(this.m_strFileName.Substring(index+0, 2)),
					Convert.ToInt32(this.m_strFileName.Substring(index+7, 2)),
					Convert.ToInt32(this.m_strFileName.Substring(index+9, 2)),
					Convert.ToInt32(this.m_strFileName.Substring(index+11, 2)),
					calendar);

			}
			catch (Exception e)
			{
				Logger.LogString("Error parsing file name: " + this.m_strFileName);
				Logger.LogString("Error message is: " + e.Message );
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown exception parsing filename: " + this.m_strFileName);
				throw;
			}
		}

		// This method checks the database to see if a duplicate exists.
		// If no duplicate exists then we return insertNewItem.
		// If duplicate exists we check if we should keep existing item or replace existing 
		// item with new item and return updateType accordingly.
		// If we are to replace existign then we assign OfficerId, CallDateTime and FileName to
		// the reference parameter so that the row can be deleted.
		private updateType checkForUpdate(ref DateTime p_dateTime)
		{
			try
			{
				PollerDB db = new PollerDB();
				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				System.DateTime dtMin = this.m_dateTimeCall.AddSeconds(-PollerConfig.m_diffSecs);
				System.DateTime dtMax = this.m_dateTimeCall.AddSeconds(PollerConfig.m_diffSecs);
				cmd.CommandText  =	" select  a.FaultDateTime as FaultDateTime " + 
									" from FaultLog a " + 
									" where a.FaultDateTime >= #" + dtMin.ToString("yyyy/MM/dd HH:mm:ss") + "# " +
									" and a.FaultDateTime <= #" + dtMax.ToString("yyyy/MM/dd HH:mm:ss") + "# " +
									" and a.FaultLocation = '" + this.m_strFaultLocation + "'" +
									" and a.Status = '" + this.m_strFaultStatus + "'";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				OdbcDataReader MyReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

				if (!MyReader.HasRows)
				{
					MyReader.Close();
					cmd.Dispose();
					db.Close();
					return updateType.InsertNewItem;
				}
				
				// Got a duplicate row, decide whether to keep it or replace it.
				// Get the returned data from the reader
				MyReader.Read();
				p_dateTime = MyReader.GetDateTime(0);

                // Check for exact Dup
                if (p_dateTime == this.m_dateTimeCall)
                {
                    Logger.LogDbgString("Exact Dup Row Found, datetime: " + p_dateTime.ToString("dd/MM/yyyy HH:mm:ss") + " FaultLocation: " + this.m_strFaultLocation + " Status:" + this.m_strFaultStatus);
                    return updateType.ExactDup;
                }

                Logger.LogDbgString("Dup Row Found, datetime: " + p_dateTime.ToString("dd/MM/yyyy HH:mm:ss") + " FaultLocation: " + this.m_strFaultLocation + " Status:" + this.m_strFaultStatus);

				// See if more than 1 row returned.
				if (MyReader.Read() )
				{
					Logger.LogString("More than 1 record returned when checking for existing record, CallDateTime in range " + 
						dtMin.ToString("dd/MM/yyyy HH:mm:ss") + " to " + dtMax.ToString("dd/MM/yyyy HH:mm:ss") );
					MyReader.Close();
					db.Close();
					return updateType.Error;
				}

				MyReader.Close();
				cmd.Dispose();
				db.Close();

				if (this.m_dateTimeCall < p_dateTime)
				{
					// This error was logged earlier than record in database
					// so replace existing
					return updateType.ReplaceExisting;
				}
				else
				{
					// This error was logged after DB record so keep existing
					return updateType.KeepExisting;
				}
			}
			catch ( Exception Ex) 
			{
				Logger.LogString("ERROR: checkForUpdate. " + Ex.Message);
			}
			catch 
			{
				Logger.LogString("ERROR: checkForUpdate. Unknown Exception");
			}
			return updateType.Error;
		}


		public void Process(string p_strFilePath)
		{
			try
			{
				parseFileName(p_strFilePath);

				// We must first figure out if this is a new record or a possible replacement for 
				// an existing record. We check to see if a record exists with the same fault location
				// and answer time. Answer times are considered equal if they lie within a certain range.
					
				DateTime dateTimeExisting = new DateTime();
				updateType res = checkForUpdate(ref dateTimeExisting);

				if (res == updateType.Error)
				{
					Exception e = new Exception("Error found checking for duplicates");
					throw e;
				}

				switch (res)
				{
					case updateType.InsertNewItem:
						InsertNewItem();
						break;
					
					case updateType.KeepExisting:
						KeepExisting();
						break;

					case updateType.ReplaceExisting:
						ReplaceExisting(dateTimeExisting);
						break;

                    case updateType.ExactDup:
                        ProcessExactDup();
                        break;
				}
			}
			catch
			{
				try
				{
					// Move file to error file dir - this shouldn't fail
					LogData.MoveErrorFile(this.m_strFilePath);
				}
				catch
				{
					// but if it does, rename the file
					Logger.LogString("Failed to move file to error dir, will rename file");
					LogData.RenameFile(this.m_strFilePath);
				}
			}
		}

        private void ProcessExactDup()
        {
            // Ignore this file, just delete it.
            Poller.DeleteFile(this.m_strFilePath);
        }

		private void ReplaceExisting(DateTime p_dateTime)
		{
			try
			{
				// Delete existing row from database
				// Then InsertNewItem
				DeleteFaultLog(p_dateTime, this.m_strFaultLocation, this.m_strFaultStatus);

				// Read description from new file 
				// insert row into database
				ReadFile();
				InsertFaultLog();
				Poller.DeleteFile(this.m_strFilePath);
			}
			catch (Exception e)
			{
				Logger.LogString("Error in ReplaceExisting, file " + this.m_strFilePath);
				Logger.LogString("Error is " + e.Message);
				throw;
			}
			catch 
			{
				Logger.LogString("Unknow exception in ReplaceExisting, file " + this.m_strFilePath);
				throw;
			}
		}


		private void KeepExisting()
		{
			try
			{
				// Delete new File
				Poller.DeleteFile(this.m_strFilePath);
			}
			catch (Exception e)
			{
				Logger.LogString("Error in KeepExisting, deleting File " + this.m_strFilePath);
				Logger.LogString("Error is " + e.Message);
				throw;
			}
			catch 
			{
				Logger.LogString("Unknow exception in KeepExisting, deleting file " + this.m_strFilePath);
				throw;
			}
		}

		private void InsertNewItem()
		{
			try
			{
				// Read description from new file 
				// insert row into database
				ReadFile();

                // Delete first to prevent duplicates
				Poller.DeleteFile(this.m_strFilePath);
                InsertFaultLog();

			}
			catch (Exception e)
			{
				Logger.LogString("Error Inserting New Item " + this.m_strFilePath);
				Logger.LogString("Error is " + e.Message);
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown exception Inserting New Item " + this.m_strFilePath);
				throw;
			}
		}

		private bool DeleteFaultLog(DateTime p_dateTime, string p_strFaultLocation, string p_strStatus)
		{
			try
			{
				PollerDB db = new PollerDB();

				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText = "DELETE FROM FaultLog WHERE " +
					"FaultDateTime = #" + p_dateTime.ToString("yyyy/MM/dd HH:mm:ss") + "# AND " +
					"FaultLocation = '" + p_strFaultLocation + "' AND " +
					"Status = '" + p_strStatus + "'";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				int Result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				db.Close();
				Logger.LogString(Result + " row(s) deleted");
			}
			catch ( Exception e) 
			{
				Logger.LogString("Error deleteing row from FaultLog table");
				Logger.LogString("Error message is: " + e.Message );
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown exception deleting row into FaultLog table");
				throw;
			}
			return true;
		}

		private void InsertFaultLog()
		{
			try
			{
				PollerDB db = new PollerDB();

				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText = "INSERT INTO FaultLog(FaultDateTime, " +
					"FaultLocation, " +
					"Description, " +
					"Status) " +
					"VALUES (" + "#" + this.m_dateTimeCall.ToString("yyyy/MM/dd HH:mm:ss") + "#, " +
					"'" + this.m_strFaultLocation + "', " +
					"'" + this.m_strFaultDescription + "', " +
					"'" + this.m_strFaultStatus + "')";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				int Result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				db.Close();
				Logger.LogInfoString(Result + " row(s) inserted");
			}
			catch ( Exception e) 
			{
				Logger.LogString("Error inserting row into FaultLog table");
				Logger.LogString("Error message is: " + e.Message );
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown exception inserting row into FaultLog table");
				throw;
			}
		}
	}
}
