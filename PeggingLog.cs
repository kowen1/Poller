using System;
using System.Data;
using System.Data.Odbc;
using System.IO;
using System.Configuration;
using Poller;

namespace Poller
{
	/// <summary>
	/// 
	/// </summary>
	public class PeggingLog
	{
		// Avon 1;250505_114408;301299_000000;-1;Reo;;AA1,AA10,AA34;False;;Forgot;Avon 1 Pegging
		//
		// 1 Pegging Route 	  Avon  1;
		// 2 Start Time 		  250505_114408;
		// 3 Actual Start Time   301299_000000;  // this date is used when no pegging points have been visited
		// 4 Time to start       -1;             // -1 indicated pegging never started 
		// 5 House Block         Reo; 
		// 6 Pegging pts visited ;               // in this case no pegging points where visited
		// 7 Pegging pts missed  AA1,AA10,AA34;  
		// 8 Pegging Complete    False; 
		// 9 User IDs            ;              // in this case on user ids
		// 10 Reason              Forgot;
		// 11 Description         Avon 1 Pegging               
	
		enum updateType { InsertNewItem, ReplaceExisting, KeepExisting, Error, ExactDup };

		private string m_strPeggingRoute;
		private string m_strStartTime;
		private string m_strActualStartTime;
		private string m_strTimeToStart;
		private string m_strHouseBlock;
		private string m_strPeggingPointsVisited;
		private string m_strPeggingPointsMissed;
		private string m_strPeggingComplete;
		private string m_strUserIDs;
		private string m_strReason;
		private string m_strDescription;
		private string m_strComments;

		private System.DateTime m_dateTimeStartTime;
		private System.DateTime m_dateTimeActualStartTime;
		private string m_strUserNamesLookup;
		private bool m_bPeggingComplete;

		private string m_strFilePath;		// Full path and file name
		private string m_strFileName;		// Just the file name

		public PeggingLog(string p_strFilePath)
		{
			// Save a copy of the full file path + file name
			this.m_strFilePath = p_strFilePath;

			// Save file name only
			this.m_strFileName = Path.GetFileName(this.m_strFilePath);

			this.m_strPeggingRoute = "";
			this.m_strStartTime = "";
			this.m_strActualStartTime = "";
			this.m_strTimeToStart = "";
			this.m_strHouseBlock = "";
			this.m_strPeggingPointsVisited = "";
			this.m_strPeggingPointsMissed = "";
			this.m_strPeggingComplete = "";
			this.m_strUserIDs = "";
			this.m_strReason = "";
			this.m_strDescription = "";
			this.m_strUserNamesLookup = "";
            this.m_strComments = "";

			Process();
		}

		public void ReadFile()
		{
			try
			{
				if (File.Exists(this.m_strFilePath) )
				{
					StreamReader sr = File.OpenText(this.m_strFilePath);
					String strData = sr.ReadToEnd();
					char[] trimarray = {'\n', '\r'};
					strData = strData.TrimEnd(trimarray);
					sr.Close();

					// Now parse string into ; seperated values
					String [] values = strData.Split(';'); 
					int cnt=1;
					foreach (String val in values) 
					{
						switch (cnt)
						{
							case 1:
								m_strPeggingRoute = val;
								break;

							case 2:
								m_strStartTime = val;
								break;

							case 3:
								m_strActualStartTime = val;
								break;

							case 4:
								m_strTimeToStart = val;
								break;

							case 5:
								m_strHouseBlock = val;
								break;

							case 6:
								m_strPeggingPointsVisited = val;
								break;

							case 7:
								m_strPeggingPointsMissed = val;
								break;

							case 8:
								m_strPeggingComplete = val;
								break;

							case 9:
								m_strUserIDs = val;
								break;

							case 10:
								m_strReason = val;
								break;

							case 11:
								m_strDescription = val;
								break;

							default:
								break;
						}
						cnt++;
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

		public void PrepareForInsert()
		{
			// Convert date strings to datetime
			// date string format is ddmmyy_ddmmss

			System.Globalization.CultureInfo info =
				new System.Globalization.CultureInfo("en-GB", false);

			System.Globalization.Calendar calendar = info.Calendar;

            if (m_strStartTime.Length != 0)
            {
                this.m_dateTimeStartTime = new System.DateTime(
                    Convert.ToInt32(this.m_strStartTime.Substring(4, 2)) + 2000,
                    Convert.ToInt32(this.m_strStartTime.Substring(2, 2)),
                    Convert.ToInt32(this.m_strStartTime.Substring(0, 2)),
                    Convert.ToInt32(this.m_strStartTime.Substring(7, 2)),
                    Convert.ToInt32(this.m_strStartTime.Substring(9, 2)),
                    Convert.ToInt32(this.m_strStartTime.Substring(11, 2)),
                    calendar);
                m_strStartTime = "#" + this.m_dateTimeStartTime.ToString("yyyy/MM/dd HH:mm:ss") + "#";
            }
            else
                m_strStartTime = "NULL";

            if (m_strActualStartTime.Length != 0)
            {
                this.m_dateTimeActualStartTime = new System.DateTime(
                    Convert.ToInt32(this.m_strActualStartTime.Substring(4, 2)) + 2000,
                    Convert.ToInt32(this.m_strActualStartTime.Substring(2, 2)),
                    Convert.ToInt32(this.m_strActualStartTime.Substring(0, 2)),
                    Convert.ToInt32(this.m_strActualStartTime.Substring(7, 2)),
                    Convert.ToInt32(this.m_strActualStartTime.Substring(9, 2)),
                    Convert.ToInt32(this.m_strActualStartTime.Substring(11, 2)),
                    calendar);
                m_strActualStartTime = "#" + this.m_dateTimeActualStartTime.ToString("yyyy/MM/dd HH:mm:ss") + "#";
            }
            else
                m_strActualStartTime = "NULL";

			// Now parse useid's string into , seperated values
			if (this.m_strUserIDs != "")
			{
				String [] values = this.m_strUserIDs.Split(','); 
				int cnt=1;
				foreach (String val in values) 
				{
					if (cnt > 1)
						this.m_strUserNamesLookup += ", ";
					string strUserName = Poller.LookupUserName(val);
					this.m_strUserNamesLookup += strUserName;
					cnt++;
				}
			}
			else
			{
				this.m_strUserNamesLookup = "";
			}

			if ( this.m_strPeggingComplete.ToLower() == "false" ||
				this.m_strPeggingComplete.ToLower() == "no")
			{
				this.m_bPeggingComplete = false;
			}
			else
			{
				this.m_bPeggingComplete = true;
			}
		}

		public void Process()
		{
			try
			{
				ReadFile();
				PrepareForInsert();
				InsertNewItem();
			}
			catch (Exception e)
			{
				Logger.LogString("Exception Processing file " + this.m_strFilePath);
				Logger.LogString(e.Message);
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


		private void InsertNewItem()
		{
			try
			{
				// insert row into database, delete file first to prevent
                // inserting duplicates if delete fails.
                Poller.DeleteFile(this.m_strFilePath);
				InsertPeggingLog();
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

		public static void DeletePeggingLog(DateTime deleteBefore)
		{
			try
			{
				PollerDB db = new PollerDB();

				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText = "DELETE FROM PeggingLog WHERE " +
					"StartDateTime < #" + deleteBefore.ToString("yyyy/MM/dd 00:00:00") + "#";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				int Result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				db.Close();
				Logger.LogString(Result + " row(s) deleted");
			}
			catch ( Exception e) 
			{
				Logger.LogString("Error deleteing row from PeggingLog table");
				Logger.LogString("Error message is: " + e.Message );
			}
			catch 
			{
				Logger.LogString("Unknown exception deleting row into PeggingLog table");
			}
		}

		private void InsertPeggingLog()
		{
			try
			{
				PollerDB db = new PollerDB();

				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText = "INSERT INTO PeggingLog(RouteName, " +
					"StartDateTime, " +
					"ActualStartDateTime, " +
					"TimetoStart, " +
					"CellsVisited, " +
					"CellsMissed, " +
					"HouseBlock, " +
					"Complete, " +
					"UserIds, " +
					"UserNames, " +
					"IncompleteReason, " +
					"Description, " +
					"Comments) " +
					"VALUES (" + 
					"'" + this.m_strPeggingRoute + "', " +
                    this.m_strStartTime + ", " +
                    this.m_strActualStartTime + ", " +
//					"#" + this.m_dateTimeStartTime.ToString("yyyy/MM/dd HH:mm:ss") + "#, " +
//					"#" + this.m_dateTimeActualStartTime.ToString("yyyy/MM/dd HH:mm:ss") + "#, " +

					this.m_strTimeToStart + ", " +
					"'" + this.m_strPeggingPointsVisited + "', " +
					"'" + this.m_strPeggingPointsMissed + "', " +
					"'" + this.m_strHouseBlock + "', " +
					this.m_bPeggingComplete + ", " +
					"'" + this.m_strUserIDs + "', " +
					"'" + this.m_strUserNamesLookup + "', " +
					"'" + this.m_strReason + "', " +
					"'" + this.m_strDescription + "', " + 
					"'" + this.m_strComments + "')";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				int Result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				db.Close();
				Logger.LogInfoString(Result + " row(s) inserted");
			}
			catch ( Exception e) 
			{
				Logger.LogString("Error inserting row into PeggingLog table");
				Logger.LogString("Error message is: " + e.Message );
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown exception inserting row into PeggingLog table");
				throw;
			}
		}
	}
}
