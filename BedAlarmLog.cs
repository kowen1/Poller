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
	public class BedAlarmLog
	{
		// R5;110605_134900;110605_140409;908;000009AE344901;Health Watch

		// R5             - Cell - look up house block
        // 110605_134900  - Alarm time
		// 110605_140409  - reset time
		// 908            - time to reset    
		// 000009AE344901 - user id - look up user name 
		// Health Watch   - reason
	
		enum updateType { InsertNewItem, ReplaceExisting, KeepExisting, Error, ExactDup };

		// Fields parsed from File.
		private string m_strCell;
		private string m_strAlarmTime;
		private string m_strResetTime;
		private string m_strTimeToReset;
		private string m_strUserId;
		private string m_strReason;

		// DB Fields
		private System.DateTime m_dbAlarmDateTime;
		private System.DateTime m_dbResetDateTime;
		private System.Int32 m_dbTimeToReset;
		private string m_dbCellLocation;
		private string m_dbHouseBlock;
		private string m_dbUserId;
		private string m_dbUserName;
		private string m_dbReason;
		private string m_dbComments;

		private string m_strFilePath;		// Full path and file name
		private string m_strFileName;		// Just the file name

		public BedAlarmLog(string p_strFilePath)
		{
			// Save a copy of the full file path + file name
			this.m_strFilePath = p_strFilePath;

			// Save file name only
			this.m_strFileName = Path.GetFileName(this.m_strFilePath);

            m_dbTimeToReset = -1;
            m_dbCellLocation = "";
            m_dbHouseBlock = "";
            m_dbUserId = "";
            m_dbUserName = "";
            m_dbReason = "";
            m_dbComments = "";

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
					foreach (String val2 in values) 
					{
						string val = val2.Trim();
						switch (cnt)
						{
							case 1:
								m_strCell = val;
								break;

							case 2:
								m_strAlarmTime = val;
								break;

							case 3:
								m_strResetTime = val;
								break;

							case 4:
								m_strTimeToReset = val;
								break;

							case 5:
								m_strUserId = val;
								break;

							case 6:
								m_strReason = val;
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

            if (m_strAlarmTime.Length != 0)
            {
                this.m_dbAlarmDateTime = new System.DateTime(
                    Convert.ToInt32(this.m_strAlarmTime.Substring(4, 2)) + 2000,
                    Convert.ToInt32(this.m_strAlarmTime.Substring(2, 2)),
                    Convert.ToInt32(this.m_strAlarmTime.Substring(0, 2)),
                    Convert.ToInt32(this.m_strAlarmTime.Substring(7, 2)),
                    Convert.ToInt32(this.m_strAlarmTime.Substring(9, 2)),
                    Convert.ToInt32(this.m_strAlarmTime.Substring(11, 2)),
                    calendar);

                m_strAlarmTime = "#" + this.m_dbAlarmDateTime.ToString("yyyy/MM/dd HH:mm:ss") + "#";
            }
            else
                m_strAlarmTime = "NULL";

            if (m_strResetTime.Length != 0)
            {
                this.m_dbResetDateTime = new System.DateTime(
                    Convert.ToInt32(this.m_strResetTime.Substring(4, 2)) + 2000,
                    Convert.ToInt32(this.m_strResetTime.Substring(2, 2)),
                    Convert.ToInt32(this.m_strResetTime.Substring(0, 2)),
                    Convert.ToInt32(this.m_strResetTime.Substring(7, 2)),
                    Convert.ToInt32(this.m_strResetTime.Substring(9, 2)),
                    Convert.ToInt32(this.m_strResetTime.Substring(11, 2)),
                    calendar);
                m_strResetTime = "#" + this.m_dbResetDateTime.ToString("yyyy/MM/dd HH:mm:ss") + "#";
            }
            else
                m_strResetTime = "NULL";


			// Convert Reset time to int32
			this.m_dbTimeToReset = Convert.ToInt32(m_strTimeToReset);

			// No conversion here
			this.m_dbCellLocation = this.m_strCell;

			// Look up Cell House Block
			this.m_dbHouseBlock = Poller.lookupHouseBlock(this.m_strCell);


			// Now parse useid into username
            if (this.m_strUserId.Length != 0)
            {
                this.m_dbUserName = Poller.LookupUserName(this.m_strUserId);
                this.m_dbUserId = "'" + m_strUserId + "'";
            }
            else
            {
                // No user if so specify username = "None"
                this.m_dbUserId = "NULL";
                this.m_dbUserName = "None";
            }

			// No conversion here
			this.m_dbReason = this.m_strReason;

			this.m_dbComments = "";
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
				// insert row into database
                // Delete first to prevent duplicates
				Poller.DeleteFile(this.m_strFilePath);
				InsertBedAlarmLog();
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

		public static void DeleteBedAlarmLog(DateTime deleteBefore)
		{
			try
			{
				PollerDB db = new PollerDB();

				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText = "DELETE FROM BedAlarmLog WHERE " +
					"AlarmDateTime < #" + deleteBefore.ToString("yyyy/MM/dd 00:00:00") + "#";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				int Result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				db.Close();
				Logger.LogString(Result + " row(s) deleted");
			}
			catch ( Exception e) 
			{
				Logger.LogString("Error deleteing row from BedAlarmLog table");
				Logger.LogString("Error message is: " + e.Message );
			}
			catch 
			{
				Logger.LogString("Unknown exception deleting row into BedAlarmLog table");
			}
		}

		private void InsertBedAlarmLog()
		{
			try
			{
				PollerDB db = new PollerDB();

				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText = "INSERT INTO BedAlarmLog(" +
					"AlarmDateTime, " +
					"ResetDateTime, " +
					"TimetoReset, " +
					"CellLocation, " +
					"HouseBlock, " +
					"UserId, " +
					"UserName, " +
					"Reason, " +
					"Comments) " +
					"VALUES (" + 
                    this.m_strAlarmTime + ", " +
                    this.m_strResetTime + ", " +
//					"#" + this.m_dbAlarmDateTime.ToString("yyyy/MM/dd HH:mm:ss") + "#, " +
//					"#" + this.m_dbResetDateTime.ToString("yyyy/MM/dd HH:mm:ss") + "#, " +
					this.m_dbTimeToReset.ToString() + ", " +
					"'" + this.m_dbCellLocation + "', " +
					"'" + this.m_dbHouseBlock + "', " +
					this.m_dbUserId + ", " +
					"'" + this.m_dbUserName + "', " +
					"'" + this.m_dbReason + "', " +
					"'" + this.m_dbComments + "')";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				int Result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				db.Close();
				Logger.LogInfoString(Result + " row(s) inserted");
			}
			catch ( Exception e) 
			{
				Logger.LogString("Error inserting row into BedAlarmLog table");
				Logger.LogString("Error message is: " + e.Message );
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown exception inserting row into BedAlarmLog table");
				throw;
			}
		}
	}
}
