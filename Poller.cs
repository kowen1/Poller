using System;
using System.Timers;
using System.IO;
using System.Data;
using System.Data.Odbc;
using System.Collections;
using System.Configuration;

namespace Poller
{
	/// <summary>
	/// Summary description for Poller.
	/// </summary>
	public class Poller
	{
		private System.Timers.Timer m_pollTimer;
		private System.Collections.ArrayList m_arrayListLocations;
		private static int m_pollInterval = Convert.ToInt32(ConfigurationSettings.AppSettings["PollInterval"]);
		private static int m_retryInterval = Convert.ToInt32(ConfigurationSettings.AppSettings["RetryInterval"]);
		private static string m_strPurgeTime = ConfigurationSettings.AppSettings["DailyPurgeTime"];
		private static bool waitingToPurge = false;
		private static bool bReadLocationErrorLogged = false;

		private System.Collections.ArrayList m_array = new ArrayList();

		public Poller()
		{
			this.m_arrayListLocations = new ArrayList();
		}

		public void Start()
		{
			this.m_pollTimer = new System.Timers.Timer(2000); // no delay to first event
			this.m_pollTimer.Elapsed += new ElapsedEventHandler(OnTimerEvent);

			this.m_pollTimer.AutoReset = false;
			this.m_pollTimer.Start();
		}

		public void Stop()
		{
			this.m_pollTimer.Stop();
		}

		public static void Purge()
		{
			if (PollerConfig.m_daysInDb != 0)
			{
				// Delete all rows with callDateTime older than now - daysInDB
				DateTime now = DateTime.Now;
				DateTime deleteBefore = now.AddDays(-(PollerConfig.m_daysInDb - 1));

				LogData.PurgeLogData(deleteBefore);
				LogData.PurgeAttendanceData(deleteBefore);
				FaultData.PurgeFaultData(deleteBefore);
				BedAlarmLog.DeleteBedAlarmLog(deleteBefore);
				PeggingLog.DeletePeggingLog(deleteBefore);

				Poller.PurgeDirOlderThan(PollerConfig.m_strMoveToDir, deleteBefore);
				Poller.PurgeDirOlderThan(PollerConfig.m_strMoveToErrorDir, deleteBefore);
			}
		}

		public static void CheckIfDeletable(string p_strFileName)
		{
			if (PollerConfig.m_daysInLocation != 0)
			{
				// Delete all rows with callDateTime older than now - daysInDB
				DateTime now = DateTime.Now;
				DateTime deleteBefore = now.AddDays(-(PollerConfig.m_daysInLocation - 1));

				FileInfo fi = new FileInfo(p_strFileName);

				if (fi.LastWriteTime < deleteBefore)
				{
					DeleteFile(p_strFileName);
					Logger.LogInfoString("Deleted File " + p_strFileName);
				}
			}
		}

		public static void DeleteFile(string p_strFileName)
		{
			FileInfo fi = new FileInfo(p_strFileName);
			if (fi.Exists)
			{
				fi.Delete();
				Logger.LogInfoString("Deleted File " + p_strFileName);
			}
			else
				Logger.LogString("Attempt to delete File " + p_strFileName + ", file does not exist");

		}

		public static string MoveFile(string p_strFilePath, string p_strDestination)
		{
			// Set up Move To dir
			// string strDestination = PollerConfig.m_strMoveToDir + "\\" + this.m_dateTimeCall.ToString("yyyy-MM-d");
			try
			{
				string strDestination = "";

				DirectoryInfo dirInf = new DirectoryInfo(p_strDestination);

				// If directory does not exist, then create it
				if (!dirInf.Exists)
				{
					Logger.LogInfoString("Created Directory " + p_strDestination);
					dirInf.Create();
				}

				FileInfo fi = new FileInfo(p_strFilePath);
				if (!fi.Exists)
				{
					string err = "Attempt to move file " + p_strDestination + "file does not exist";
					Logger.LogString(err);
					System.Exception e = new Exception(err);
					throw e;
				}
				else
				{
					strDestination = p_strDestination + "\\" + fi.Name;
				
					// Now move file to directory
					fi.MoveTo(strDestination);
					Logger.LogInfoString("Moved File " + p_strFilePath + " to " + p_strDestination);
				}
				return strDestination;
			}
			catch (Exception e)
			{
				Logger.LogString("Error Moving file " + p_strFilePath + " to " + p_strDestination);
				Logger.LogString("Error message is: " + e.Message );
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown exception Moving file from " + p_strFilePath + " to " + p_strDestination);
				throw;
			}
		}



#if CRAP
		public static void PurgeErrorFiles(DateTime deleteBefore)
		{
			DirectoryInfo di = new DirectoryInfo(PollerConfig.m_strMoveToErrorDir);
			try
			{
				FileInfo[] files = di.GetFiles();

				foreach (System.IO.FileInfo fi in files)
				{
					DateTime dtCreate = fi.CreationTime;
					if (dtCreate < deleteBefore)
					{
						try
						{
							if (fi.Exists)
							{
								fi.Delete();
								Logger.LogInfoString("Purged Error file " + fi.FullName);
							}
							else
							{
								string err = "Cannot purge Error file " + fi.FullName + " as it doesn't exist";
								Logger.LogString(err);
								System.Exception e = new Exception(err);
								throw e;
							}
						}
						catch (Exception e)
						{
							Logger.LogString("Exception while deleting file, " + fi.FullName + " will continue with next file");
							Logger.LogString("Message is " + e.Message);
							throw;
						}
						catch
						{
							Logger.LogString("Unknown Exception while deleting file, " + fi.FullName + " will continue with next file");
							throw;
						}
					}
				}
			}
			catch (Exception e)
			{
				Logger.LogString("Exception while purgign Error files before " + deleteBefore.ToString("yyyy/MM/dd"));
				Logger.LogString("Message is " + e.Message);
				throw;
			}
			catch
			{
				Logger.LogString("Unknown Exception while purging Error files before " + deleteBefore.ToString("yyyy/MM/dd"));
				throw;
			}
		}
#endif

		public static void PurgeDirOlderThan(string pDir, DateTime deleteBefore)
		{
			// Now cleanup any directories older than DaysInDB that may
			// be present in pDir

			// All files in this directory must go
			DirectoryInfo di2 = new DirectoryInfo(pDir);
			DirectoryInfo[] dirs = di2.GetDirectories();
			FileInfo[] files2 = di2.GetFiles();

			foreach (System.IO.DirectoryInfo di in dirs)
			{
				if (di.LastWriteTime < deleteBefore)
				{
					try
					{
						// Delete the lot recursively
						LogAndDeleteFilesInDir(di);
					}
					catch
					{
						Logger.LogInfoString("Couldn't delete directory " + di.FullName);
					}
				}
			}

			// Repeat for any files
			foreach (System.IO.FileInfo fi in files2)
			{
				if (fi.LastAccessTime < deleteBefore)
				{
					try
					{
						// Delete the file
						fi.Delete();
						Logger.LogInfoString("Deleted file " + fi.FullName);
					}
					catch
					{
						Logger.LogInfoString("Couldn't delete file " + fi.FullName);
					}
				}
			}
		}


		private static void LogAndDeleteFilesInDir(DirectoryInfo di)
		{
			FileInfo[] files2 = di.GetFiles();
			DirectoryInfo[] dirs = di.GetDirectories();

			foreach (System.IO.DirectoryInfo di2 in dirs)
			{
				LogAndDeleteFilesInDir(di2);
			}

			foreach (System.IO.FileInfo fi in files2)
			{
				Logger.LogInfoString("Deleting file " + fi.FullName);
				fi.Delete();
			}
			Logger.LogInfoString("Deleting directory " + di.FullName);
			di.Delete();
		}


		public static string LookupUserName(string p_strUserId)
		{
			try
			{
				PollerDB db = new PollerDB();
				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText  = "SELECT  UserName " + 
					"FROM Users " + 
					"WHERE UserId = '" + p_strUserId.Trim() + "'";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				OdbcDataReader MyReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

				if (!MyReader.HasRows)
				{
					string userName = "Unknown";
					MyReader.Close();
					cmd.Dispose();
					db.Close();
					Poller.InsertUserId(p_strUserId, userName);
					return userName;
				}
				
				MyReader.Read();
				string strUserName = MyReader.GetString(0);

				// If more than 1 row returned report error and return 1st row found
				if (MyReader.Read() )
				{
					MyReader.Close();
					cmd.Dispose();
					db.Close();
					System.Exception e = new System.Exception("LookupUseId2: Multiple Rows in DB for UseId" + p_strUserId);
					throw e;
				}

				MyReader.Close();
				cmd.Dispose();
				db.Close();
				return strUserName;
			}
			catch ( Exception e) 
			{
				Logger.LogString("Error looking up UserName with UserId " + p_strUserId);
				Logger.LogString("Error message is: " + e.Message );
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown exception looking up UserName with UserId " + p_strUserId);
				throw;
			}
		}

		public static string lookupHouseBlock(string p_strCellLocation)
		{
			try
			{
				string strHouseBlockName = "Not Found in DB";

				PollerDB db = new PollerDB();
				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText  = "SELECT  HouseBlockName " + 
					"FROM HouseBlockLocation " + 
					"WHERE HouseId = '" + p_strCellLocation.Substring(0, 1) + "'";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				OdbcDataReader MyReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

				if (!MyReader.HasRows)
				{
					MyReader.Close();
					cmd.Dispose();
					db.Close();
					System.Exception e = new System.Exception("LookupHouseBlock: No match in DB for CellLocation" + p_strCellLocation);
					throw e;
				}
				
				MyReader.Read();
				strHouseBlockName = MyReader.GetString(0);

				// If more than 1 row returned report error and return 1st row found
				if (MyReader.Read() )
				{
					Logger.LogString("More than 1 record returned when looking up HouseBlockName with CellLocation " + p_strCellLocation);
					MyReader.Close();
					cmd.Dispose();
					db.Close();
					System.Exception e = new System.Exception("LookupHouseBlock: Multiple Rows in DB for CellLocation" + p_strCellLocation);
					throw e;
				}

				MyReader.Close();
				cmd.Dispose();
				db.Close();
				return strHouseBlockName;
			}
			catch ( Exception e) 
			{
				Logger.LogString("Error looking up HouseBLock with CellLocation " + p_strCellLocation);
				Logger.LogString("Error message is: " + e.Message );
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown exception looking up HouseBLock with CellLocation " + p_strCellLocation);
				throw;
			}
		}

		public static void InsertUserId(string p_strUserId, string p_strUserName)
		{
			try
			{
				PollerDB db = new PollerDB();

				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText = "INSERT INTO Users(UserId, UserName) " +
					"VALUES ('" + p_strUserId.Trim() + "', '" +
					p_strUserName + "')";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				int Result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				db.Close();
				Logger.LogDbgString(Result + " row(s) inserted");
			}
			catch ( Exception e) 
			{
				string err = "Error inserting user id " + p_strUserId + " into Users table";
				Logger.LogString("err");
				Logger.LogString("Error message is: " + e.Message );
				throw e;
			}
			catch 
			{
				string err = "Error inserting user id " + p_strUserId + " into Users table";
				Logger.LogString("err");
				System.Exception e = new Exception(err);
				throw e;
			}
		}


		private void OnTimerEvent(object source, ElapsedEventArgs e) 
		{
			try
			{
				// Check to see if time to purge, if it is go and do it, 
				// note, polling stops while purge in progress
				TimeSpan timeNow = DateTime.Now.TimeOfDay;
				TimeSpan timePurge = new TimeSpan(Convert.ToInt32(m_strPurgeTime.Substring(0, 2)), 
					Convert.ToInt32(m_strPurgeTime.Substring(3, 2)), 0);
				
				if (timeNow < timePurge)
					waitingToPurge = true;
				else
				{
					if (waitingToPurge)
					{
						waitingToPurge = false;
						Purge();
					}
				}

				if ( !ReadLocationTable() )
				{
					if ( !bReadLocationErrorLogged )
					{
						bReadLocationErrorLogged = true;
						Logger.LogString("Failed to read Location table, will retry after " + m_retryInterval/1000 + " seconds");
					}				
					this.m_pollTimer.Interval = m_retryInterval;
					this.m_pollTimer.Start();	// Retry 
					return;
				}

				if (bReadLocationErrorLogged)
					Logger.LogString("Location table read succesfully");

				bReadLocationErrorLogged = false;

				// Now for each Location check for files if status enabled
				for (int i=0; i < this.m_arrayListLocations.Count; i++)
				{
					structLocation sLoc = (structLocation)m_arrayListLocations[i];
					if (sLoc.Status.ToLower() == "enabled")
					{
						CheckForFiles(ref sLoc);
						m_arrayListLocations[i] = (structLocation)sLoc;

					}
				}
				this.m_pollTimer.Interval = m_pollInterval;
				this.m_pollTimer.Start();

				//			Console.WriteLine("Tot Mem before collect= " + GC.GetTotalMemory(false) );
				GC.Collect();
				//			Console.WriteLine("Tot Mem after collect = " + GC.GetTotalMemory(false) );
			}
			catch
			{
				Logger.LogString("Onevent caught an excpetion");
			}
		}

		private void CheckForFiles(ref structLocation sLoc)
		{
			DirectoryInfo di = new DirectoryInfo(sLoc.Directory);
            try
            {
                FileInfo[] files = di.GetFiles();

                foreach (System.IO.FileInfo fi in files)
                {
                    string ext = fi.Extension.ToLower();
                    // Ignore .err files as these are files that cannot be moved
                    // to error file dir and have been renamed by us.
					if (ext.Substring(0, 4) == ".err")
					{
						CheckIfDeletable(fi.FullName);
						continue;
					}

                    // Ignore .tmp files as these are .wav files being processed
					if (ext.Substring(0, 4) == ".tmp")
					{
						CheckIfDeletable(fi.FullName);
						continue;
					}

                    switch (ext )
                    {
						case ".peg":
							PeggingLog peg = new PeggingLog(fi.FullName);
							break;

						case ".bed":
							BedAlarmLog bed = new BedAlarmLog(fi.FullName);
							break;

                        case ".wav":
                        case ".mp3":
                        case ".dat":
                            LogData ld = new LogData(fi.FullName, sLoc.Site, sLoc.Name);
                            break;

                        case ".flt":
                        case ".acc":
                        case ".clr":
                            FaultData flt = new FaultData(fi.FullName);
                            break;

						default:
							CheckIfDeletable(fi.FullName);
							break;
                    }
                }
				if (sLoc.bErrorLogged)
					Logger.LogString("Directory " + sLoc.Directory + " read successfully");

//				sLoc.bErrorLogged = false;
//				Console.WriteLine("Tot Mem before collect= " + GC.GetTotalMemory(false) );
				GC.Collect();
//				Console.WriteLine("Tot Mem after collect = " + GC.GetTotalMemory(false) );
            }   
            catch(Exception e)
            {
				if (!sLoc.bErrorLogged)
				{
					Logger.LogString("Exception getting files from " + sLoc.Directory);
					Logger.LogString("Message is " + e.Message);
					sLoc.bErrorLogged = true;
				}
            }
            catch
            {
				if (!sLoc.bErrorLogged)
				{
					Logger.LogString("Unknown exception getting files from " + sLoc.Directory);
					sLoc.bErrorLogged = true;
				}
            }
		}

        private void disableLocation(structLocation p_sLoc)
        {
            try
            {
                PollerDB db = new PollerDB();

                System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
                cmd.Connection = db.Connection;
                cmd.CommandText = "UPDATE Location SET Status = 'Disabled' WHERE Name = '" + p_sLoc.Name + "'";
                cmd.CommandType = CommandType.Text;
                Logger.LogDbgString(cmd.CommandText);
                int Result = cmd.ExecuteNonQuery();
				cmd.Dispose();
                db.Close();
                Logger.LogInfoString(Result + " row(s) updated");
            }
            catch ( Exception e) 
            {
                Logger.LogString("Error updating Location table ");
                Logger.LogString("Error message is: " + e.Message );
            }
            catch 
            {
                Logger.LogString("Unknown exception updaing Location table");
            }
        }
                                    

		private bool ReadLocationTable()
		{
			try
			{
				PollerDB db = new PollerDB();

				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText = "SELECT a.Name, a.DirectoryName, a.Status, a.Site " +
					"FROM Location a "; //+
					//"ORDER BY a.Name ";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				OdbcDataReader MyReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

				ArrayList tmpAL = new ArrayList();
				//this.m_arrayListLocations.Clear();
				while (MyReader.Read() )
				{
					structLocation sl = new structLocation(MyReader.GetString(0),
						MyReader.GetString(1),
						MyReader.GetString(2),
						MyReader.GetString(3) );
					int index = this.m_arrayListLocations.BinarySearch(sl);
					if (index < 0)
						tmpAL.Add(sl);
					else
					{
						// Remember errorLogged flag
						sl.bErrorLogged = ((structLocation)this.m_arrayListLocations[index]).bErrorLogged;
						tmpAL.Add(sl);
					}
				}
				this.m_arrayListLocations.Clear();
				this.m_arrayListLocations = tmpAL;
				MyReader.Close();
				cmd.Dispose();
				db.Close();
			}
			catch ( Exception e) 
			{
				Logger.LogString("Error reading Location table ");
				Logger.LogString("Error message is: " + e.Message );
				return false;
			}
			catch 
			{
				Logger.LogString("Unknown exception inserting row into Location table");
				return false;
			}
			return true;
		}
	}

	public struct structLocation : IComparable
	{
		public string Name, Directory, Status, Site;
		public bool bErrorLogged;

		public structLocation(string p_Name, string p_Directory, string p_Status, string p_Site)
		{
			this.Name = p_Name;
			this.Directory = p_Directory;
			this.Status = p_Status;
			this.Site = p_Site;
			this.bErrorLogged = false;
		}

		public int CompareTo(object obj) 
		{
			structLocation sLoc = (structLocation)obj;
			return Name.CompareTo(sLoc.Name);
		}
	}
}


