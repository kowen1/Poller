using System;
using System.Data;
using System.Data.Odbc;
using System.IO;
using System.Configuration;
using System.Collections;


namespace Poller
{
	/// <summary>
	/// 
	/// </summary>
	public class LogData
	{
		enum updateType { InsertNewItem, ReplaceExisting, KeepExisting, Error, ExactDup };

		private long m_lCallDuration;
		private string m_strCellLocation;
		private string m_strFilePath;
		private string m_strFileName;
		private string m_strHouseBlockName;
		private string m_strUserId;
		private string m_strUserName;
		private long m_lTimeToAnswer;
		private System.DateTime m_dateTimeAnswer;
		private System.DateTime m_dateTimeCall;
		private System.DateTime m_dateTimeEndCall;
		private string m_strSite;
		private string m_strLocName;
		private bool m_bIsAudio;


		public LogData(string p_strFilePath, string p_strSite, string p_strLocName)
		{
			Process(p_strFilePath, p_strSite, p_strLocName);
		}


		public static string MoveErrorFile(string p_strFilePath)
		{
			// Set up Move To Error dir
			string strDestination = PollerConfig.m_strMoveToErrorDir;

			try
			{
				DirectoryInfo dirInf = new DirectoryInfo(strDestination);

				// If directory does not exist, then create it
				if (!dirInf.Exists)
				{
					Logger.LogInfoString("Created Directory " + strDestination);
					dirInf.Create();
				}

				FileInfo fi = new FileInfo(p_strFilePath);
				if (!fi.Exists)
				{
					Logger.LogString("Attempt to move error file " + strDestination + "file does not exist");
				}
				else
				{
					strDestination = strDestination + "\\" + fi.Name;

                    if (File.Exists(strDestination) )
                    {
                        // Rename file 
                        int i = 0;
                        while (true)
                        {
                            if ( !File.Exists(strDestination + ".err" + i) )
                                break;
                            i++;
                        }
                        strDestination = strDestination + ".err" + i;
                    }

					// Now move file to directory
					fi.MoveTo(strDestination);
					Logger.LogInfoString("Moved File " + p_strFilePath + " to " + strDestination);
				}
			}
			catch (Exception e)
			{
				Logger.LogString("Error Moving file " + p_strFilePath + " to " + strDestination);
				Logger.LogString("Error message is: " + e.Message );
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown exception Moving file from " + p_strFilePath + " to " + strDestination);
				throw;
			}
			return strDestination;
		}

		public static void RenameFile(string p_strFilePath)
		{
			// Rename the file to prevent it being processed again.
			string strDestination = p_strFilePath;
			try
			{
				FileInfo fi = new FileInfo(p_strFilePath);
				if (!fi.Exists)
				{
					Logger.LogString("Attempt to rename error file " + p_strFilePath + ", file does not exist");
				}
				else
				{
                    int i = 0;
                    while (true)
                    {
                        if ( !File.Exists(strDestination + ".err" + i) )
                            break;
                        i++;
                    }
                    strDestination = strDestination + ".err" + i;

					fi.MoveTo(strDestination);
					Logger.LogInfoString("Renamed File " + p_strFilePath + " to " + strDestination);
				}
			}
			catch (Exception e)
			{
				Logger.LogString("Error Renaming file " + p_strFilePath + " to " + strDestination);
				Logger.LogString("Error message is: " + e.Message );
			}
			catch 
			{
				Logger.LogString("Unknown exception Renaming file from " + p_strFilePath + " to " + strDestination);
			}
		}


		private void parseFileName(string p_strFilePath)
		{
			try
			{
				this.m_strFilePath = p_strFilePath;

				this.m_strFileName = Path.GetFileName(p_strFilePath);
				string ext = Path.GetExtension(p_strFilePath).ToLower();
				this.m_bIsAudio = false;
				if ((ext == ".mp3") || (ext == ".wav"))
					this.m_bIsAudio = true;

//				this.m_bLocal = false;
//				string drive = Path.GetFullPath(p_strFilePath).Substring(0, 3).ToLower();
//				if (drive == @"c:\")
//					this.m_bLocal = true;

				
				// get cell location by looking for first _
				int index = this.m_strFileName.IndexOf("_");
				if (index == -1)
				{
					System.Exception e = new System.Exception("Invalid Filename: no _ found after xxxx");
					throw e;
				}
				this.m_strCellLocation = this.m_strFileName.Substring(0, index);
				
				// Now get datetime
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


				// Now get Time to Answer call
				index += 13;
				if (this.m_strFileName[index] != '_')
				{
					// If it's not _ it must be a ., anything else is an error
					if (this.m_strFileName[index] != '.')
					{
						System.Exception e = new System.Exception("Invalid Filename: no _ found after yy");
						throw e;
					}
					else
					{
						// Set Time to answer, duration and user id all 0
						this.m_lTimeToAnswer = 0;
						this.m_lCallDuration = 0;
						this.m_strUserId = "";
						this.m_strUserId = this.m_strUserId.PadRight(14, '0');
					}
				}
				else
				{
					index +=1;
					// We are now at X xxx_dddddd_tttttt_X
					int index2 = this.m_strFileName.IndexOf('_', index);
					if (index2 == -1)
					{
						// If no _ found then check for a .
						index2 = this.m_strFileName.IndexOf('.', index);
						if (index == -1)
						{
							System.Exception e = new System.Exception("Invalid Filename: no . found");
							throw e;
						}
						// If it's 14 digits then time to answer, duration are 0
						// and we treate as user id
						if (index2 - index == 14)
						{
							this.m_lTimeToAnswer = 0;
							this.m_lCallDuration = 0;
							this.m_strUserId = this.m_strFileName.Substring(index, index2-index);
						}
						else
						{
							// Try to convert it to time to answer and set duration and user id to 0
							this.m_lTimeToAnswer = Convert.ToInt32(this.m_strFileName.Substring(index, index2-index));
							this.m_lCallDuration = 0;
							this.m_strUserId = "";
							this.m_strUserId = this.m_strUserId.PadRight(14, '0');
						}
					}
					else
					{
						// We found time to answer
						this.m_lTimeToAnswer = Convert.ToInt32(this.m_strFileName.Substring(index, index2-index));

						// Now get Duration
						index2 += 1;
						index = this.m_strFileName.IndexOf('_', index2);
						if (index == -1)
						{
							// No _ means there is either a) no UserId field or b) no duration field and 1 14 digit user id, 
							// so look for . instead
							index = this.m_strFileName.IndexOf('.', index2);
							if (index == -1)
							{
								System.Exception e = new System.Exception("Invalid Filename: no . found after jj");
								throw e;
							}

							// If the string is 14 digits then we have a user id and a 0 call duration
							if (index - index2 == 14)
							{
								this.m_strUserId = this.m_strFileName.Substring(index2, index - index2);
								this.m_lCallDuration = 0;
							}
							else
							{
								// It's not 14 so we have a duration and no user id.
								this.m_lCallDuration = Convert.ToInt32(this.m_strFileName.Substring(index2, index - index2));

								// Set UserId to all 0's
								this.m_strUserId = "";
								this.m_strUserId = this.m_strUserId.PadRight(14, '0');
							}
						}
						else
						{
							// we have a _ so this is call duration
							this.m_lCallDuration = Convert.ToInt32(this.m_strFileName.Substring(index2, index-index2));

							// Now get UserId - 14 digits of hex
							index += 1;
							index2 = this.m_strFileName.IndexOf('.', index);
							if (index2 == -1)
							{
								System.Exception e = new System.Exception("Invalid Filename: no . found after ppppppp");
								throw e;
							}
							if (index2 == index)
							{
								// Set UserId to all 0's
								this.m_strUserId = "";
								this.m_strUserId = this.m_strUserId.PadRight(14, '0');
							}
							else
							{
								if (index2 - index != 14)
								{
									System.Exception e = new System.Exception("Invalid Filename: user id not exactly 14 digits");
									throw e;
								}
								this.m_strUserId = this.m_strFileName.Substring(index, index2-index);
							}
						}
					}
				}

				// Now calculate time to answer
				this.m_dateTimeAnswer = this.m_dateTimeCall.AddSeconds(this.m_lTimeToAnswer);

				// If duration is 0 we must derive from create time of file.
				if (this.m_lCallDuration == 0)
				{
					FileInfo fi = new FileInfo(p_strFilePath);
					System.DateTime dateTimeLastWrite = fi.LastWriteTime;
					TimeSpan ts = dateTimeLastWrite.Subtract(this.m_dateTimeAnswer);
					this.m_lCallDuration = (long)ts.TotalSeconds;
				}

				// Now calculate end call time
				this.m_dateTimeEndCall = this.m_dateTimeAnswer.AddSeconds(m_lCallDuration);

				// If user id is all 0's then substitute user id for location
				if (this.m_strUserId == new string('0', 14) )
					this.m_strUserId = this.m_strLocName;

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

		private bool DeleteRow(DateTime p_dateTimeCall, string p_strCellLocation)
		{
			try
			{
				PollerDB db = new PollerDB();

				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText = "DELETE FROM CallLog WHERE " +
					"CallDateTime = #" + p_dateTimeCall.ToString("yyyy/MM/dd HH:mm:ss") + "# AND " +
					"CellLocation = '" + p_strCellLocation + "'";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				int Result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				db.Close();
				Logger.LogInfoString(Result + " row(s) deleted");
				return true;
			}
			catch ( Exception e) 
			{
				Logger.LogString("Error deleteing row from CallLog table");
				Logger.LogString("Error message is: " + e.Message );
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown exception deleting row into CallLog table");
				throw;
			}
		}

		// This method checks the database to see if a duplicate exists.
		// If no duplicate exists then we return insertNewItem.
		// If duplicate exists we check if we should keep existing item or replace existing 
		// item with new item and return updateType accordingly.
		// If we are to replace existign then we assign OfficerId, CallDateTime and FileName to
		// the reference parameter so that the row can be deleted.
		private updateType checkForUpdate(ref DateTime p_dateTime, ref string p_strFileName, ref bool p_bIsAudio)
		{
			try
			{
				PollerDB db = new PollerDB();
				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				System.DateTime dtMin = this.m_dateTimeCall.AddSeconds(-PollerConfig.m_diffSecs);
				System.DateTime dtMax = this.m_dateTimeCall.AddSeconds(PollerConfig.m_diffSecs);
				cmd.CommandText  =	" select  a.CallDateTime as CallDateTime, " + 
									" a.UserId as UserId, " +
									" a.FileLocation as FileLocation, " +
									" a.Site as Site " + 
									" from CallLog a " + 
									" where a.CallDateTime >= #" + dtMin.ToString("yyyy/MM/dd HH:mm:ss") + "# " +
									" and a.CallDateTime <= #" + dtMax.ToString("yyyy/MM/dd HH:mm:ss") + "# " +
									" and a.CellLocation = '" + this.m_strCellLocation + "'" +
									" and a.Site IS NOT NULL ";
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
				string strSite;
				MyReader.Read();
				p_dateTime = MyReader.GetDateTime(0);
				string strUserId = MyReader.GetString(1);
				p_strFileName = MyReader.GetString(2);
				strSite = MyReader.GetString(3);

				bool bDBLocal = false;
				if (strSite.ToLower() == "local")
					bDBLocal = true;

				bool bNewLocal = false;
				if (this.m_strSite.ToLower() == "local")
					bNewLocal = true;

                // See if exaxt duplicate
                if (p_dateTime == this.m_dateTimeCall )
                {
                    Logger.LogDbgString("Exact Dup Row Found, datetime: " + p_dateTime.ToString("dd/MM/yyyy HH:mm:ss") + " CellLocation: " + this.m_strCellLocation + " Filename: " + p_strFileName);
					db.Close();
					cmd.Dispose();
                    return updateType.ExactDup;
                }

                Logger.LogDbgString("Dup Row Found, datetime: " + p_dateTime.ToString("dd/MM/yyyy HH:mm:ss") + " CellLocation: " + this.m_strCellLocation + " Filename: " + p_strFileName);

				// See if more than 1 row returned.
				if (MyReader.Read() )
				{
					Logger.LogString("More than 1 record returned when checking for existing record, CallDateTime in range " + 
						dtMin.ToString("dd/MM/yyyy HH:mm:ss") + " to " + dtMax.ToString("dd/MM/yyyy HH:mm:ss") );
					MyReader.Close();
					cmd.Dispose();
					db.Close();
					return updateType.Error;
				}

				MyReader.Close();
				cmd.Dispose();
				db.Close();

                // If user id's are same then 
                // If existing record is remote then keep existing






				// See if we have local file by simply checking for c:
				// Remote files must be mapped to another drive or by a URL
				string ext = Path.GetExtension(p_strFileName);
				if ((ext == ".mp3") || (ext == ".wav"))
					p_bIsAudio = true;

//				bool bLocal = false;
//				if (Path.GetFullPath(p_strFileName).Substring(0, 3) == "c:\\")
//					bLocal = true;

				if (!p_bIsAudio)
				{
					// No audio in db record
					if (this.m_bIsAudio)
					{
						// New file has audio so replace existing
						return updateType.ReplaceExisting;
					}
				}
				else
				{
					// db record is audio so check new file isn't
					if (this.m_bIsAudio)
					{
						Logger.LogString("ERROR: New record and DB record both contain audio, CallDateTime in range " + 
							dtMin.ToString("dd/MM/yyyy HH:mm:ss") + " to " + dtMax.ToString("dd/MM/yyyy HH:mm:ss") );
						return updateType.Error;
					}
					else
					{
						// keep existing as this file has no audio 
						Logger.LogInfoString("Discarded File " + this.m_strFilePath + " as duplicate DB record found with audio " +
							"Call Date Time: " + p_dateTime.ToString("dd/MM/yyyy HH:mm:ss") + " UserId: " + strUserId);
						return updateType.KeepExisting;
					}
				}

				// No audio on DB record or new file so check for a non zero User id
				if (Convert.ToUInt64(strUserId, 16) == 0)
				{
					// zero UserId
					if (Convert.ToUInt64(this.m_strUserId, 16) != 0)
					{
						// New file has non zero UserId so replace existing
						return updateType.ReplaceExisting;
					}
				}
				else
				{
					// DB record has non zero UserId
					if (Convert.ToUInt64(this.m_strUserId, 16) == 0)
					{
						// New file has zero UserId so keep existing
						Logger.LogInfoString("Discarded File " + this.m_strFilePath + " as duplicate DB record found with non zero UserId " +
							"Call Date Time: " + p_dateTime.ToString("dd/MM/yyyy HH:mm:ss") + " Officer Id: " + strUserId);
						return updateType.KeepExisting;
					}
				}

				// Check for remote file if we get here
				if (bDBLocal && !bNewLocal)
				{
					// New file is remote so replace existing
					return updateType.ReplaceExisting;
				}

				if (!bDBLocal && bNewLocal)
				{
					// DB record is remote so keep existing
					Logger.LogInfoString("Discarded File " + this.m_strFilePath + " as duplicate DB record is a remote file " +
						"Call Date Time: " + p_dateTime.ToString("dd/MM/yyyy HH:mm:ss") + " UserId: " + strUserId);
					return updateType.KeepExisting;
				}

				// If we get here both files are either local or remote
				if (bDBLocal)
				{
					Logger.LogString("ERROR: New record and DB record both local, " +
						"CallDateTime DB record: " + p_dateTime.ToString("dd/MM/yyyy HH:mm:ss") +
						" CallDateTime new file: " + this.m_dateTimeCall.ToString("dd/MM/yyyy HH:mm:ss") );
					return updateType.Error;
				}
				else
				{
					Logger.LogString("ERROR: New record and DB record both remote, " +
						"CallDateTime DB record: " + p_dateTime.ToString("dd/MM/yyyy HH:mm:ss") +
						" CallDateTime new file: " + this.m_dateTimeCall.ToString("dd/MM/yyyy HH:mm:ss") );
					return updateType.Error;
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


		// Check attendance log for duplicate, an entry with same CellLocation occuring
		// within a range of time around m_dateTimeCall
		private updateType CheckForAttLogDup()
		{
			try
			{
				PollerDB db = new PollerDB();
				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				System.DateTime dtMin = this.m_dateTimeCall.AddSeconds(-PollerConfig.m_diffSecs);
				System.DateTime dtMax = this.m_dateTimeCall.AddSeconds(PollerConfig.m_diffSecs);
				cmd.CommandText  =	" select  a.CallDateTime as CallDateTime " + 
					" from AttendanceLog a " + 
					" where a.CallDateTime >= #" + dtMin.ToString("yyyy/MM/dd HH:mm:ss") + "# " +
					" and a.CallDateTime <= #" + dtMax.ToString("yyyy/MM/dd HH:mm:ss") + "# " +
					" and a.CellLocation = '" + this.m_strCellLocation + "'" +
					" and a.UserId = '" + this.m_strUserId + "'";
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

				// Got a duplicate so keep existing
				MyReader.Close();
				cmd.Dispose();
				db.Close();
				return updateType.KeepExisting;
			}
			catch ( Exception Ex) 
			{
				Logger.LogString("ERROR: checkForAttendanceDup. " + Ex.Message);
			}
			catch 
			{
				Logger.LogString("ERROR: checkForAttendanceDup. Unknown Exception");
			}
			return updateType.Error;
		}


		public void Process(string p_strFilePath, string p_strSite, string p_strLocName)
		{
			try
			{
				this.m_strSite = p_strSite;
				this.m_strLocName = p_strLocName;

				parseFileName(p_strFilePath);

				// We must first figure out if this is a new record or a possible replacement for 
				// an existing record. We check to see if a record exists with the same cell location
				// and answer time. Answer times are considered equal if they lie within a certain range.
					
				string strFileNameExisting = "";
				DateTime dateTimeExisting = new DateTime();
				bool bIsAudio = false;

                updateType res = updateType.InsertNewItem;
                if (!this.m_bIsAudio)
                {
                    // Only check for duplicates if not audio
                    //res = checkForUpdate(ref dateTimeExisting, ref strFileNameExisting, ref bIsAudio);

                    // Check for attendance dup
                    res = CheckForAttLogDup();
                }

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
						ReplaceExisting(dateTimeExisting, strFileNameExisting, bIsAudio);
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
					MoveErrorFile(this.m_strFilePath);
				}
				catch
				{
					// but if it does, rename the file
					Logger.LogString("Failed to move file to error dir, will rename file");
					RenameFile(this.m_strFilePath);
				}
			}
		}

        private void ProcessExactDup()
        {
            // Row already in database so just attempt to move file.
            // If this fails say because file is a real duplicate then file will get moved to error file dir

			// Set up Move To dir
			string strDestination = PollerConfig.m_strMoveToDir + "\\" + this.m_dateTimeCall.ToString("yyyy-MM-d");

            Poller.MoveFile(this.m_strFileName, strDestination);
        }

		private void ReplaceExisting(DateTime p_dateTime, string p_strFileName, bool p_bIsAudio)
		{
			try
			{
				// Delete existing file if audio
				// Delete existing row from database
				// Then InsertNewItem
				if (p_bIsAudio)
					Poller.DeleteFile(p_strFileName);

				DeleteRow(p_dateTime, this.m_strCellLocation);

				InsertNewItem();
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
				// Move new file to MoveToDir
				// update strFileName with the new full path
				// insert row into database
				if (this.m_bIsAudio)
				{
					// Set up Move To dir
					string strDestination = PollerConfig.m_strMoveToDir + "\\" + this.m_dateTimeCall.ToString("yyyy-MM-d");

					this.m_strFilePath = Poller.MoveFile(this.m_strFilePath, strDestination);
					insertCallLog(this.m_strFilePath);
				}
				else
				{
					// .DAT file
					// If time to answer is not 0 then put in call log as well
					if (this.m_lTimeToAnswer != 0)
						insertCallLog("");

                    // Delete first to prevent duplicates.
					Poller.DeleteFile(this.m_strFilePath);
					insertAttendanceLog();
				}
			}
			catch (Exception e)
			{
				Logger.LogString("Error Inserting New Item " + this.m_strFilePath);
				Logger.LogString("Error is " + e.Message);
				throw;
			}
			catch 
			{
				Logger.LogString("Unknow exception Inserting New Item " + this.m_strFilePath);
				throw;
			}
		}


		private void insertCallLog(string p_strFilePath)
		{
			try
			{
				PollerDB db = new PollerDB();

				this.m_strHouseBlockName = Poller.lookupHouseBlock(this.m_strCellLocation);
				this.m_strUserName = Poller.LookupUserName(this.m_strUserId);

				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText = "INSERT INTO CallLog(AnswerDateTime, " +
					"CallDateTime, " +
					"CallDuration, " +
					"CellLocation, " +
					"EndOfCallDateTime, " +
					"FileLocation, " +
					"HouseBlock, " +
					"UserId, " +
					"UserName, " +
					"TimeToAnswer, " +
					"Site) " +
					"VALUES (" + "#" + this.m_dateTimeAnswer.ToString("yyyy/MM/dd HH:mm:ss") + "#, " +
							"#" + this.m_dateTimeCall.ToString("yyyy/MM/dd HH:mm:ss") + "#, " +
							this.m_lCallDuration + ", " +
							"'" + this.m_strCellLocation + "', " +
							"#" + this.m_dateTimeEndCall.ToString("yyyy/MM/dd HH:mm:ss") + "#, " +
							"'" + p_strFilePath + "', " +
							"'" + this.m_strHouseBlockName + "', " +
							"'" + this.m_strUserId + "', " +
							"'" + this.m_strUserName + "', " +
							this.m_lTimeToAnswer + ", " +
							"'" + this.m_strSite + "')";

				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				int Result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				db.Close();
				Logger.LogDbgString(Result + " row(s) inserted");
			}
			catch ( Exception e) 
			{
				Logger.LogString("Error inserting row into CallLog table");
				Logger.LogString("Error message is: " + e.Message );
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown exception inserting row into CallLog table");
				throw;
			}
		}
	

		private void insertAttendanceLog()
		{
			try
			{
				PollerDB db = new PollerDB();

				this.m_strHouseBlockName = Poller.lookupHouseBlock(this.m_strCellLocation);
				this.m_strUserName = Poller.LookupUserName(this.m_strUserId);

				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText = "INSERT INTO AttendanceLog(CallDateTime, " +
					"CellLocation, " +
					"HouseBlock, " +
					"UserId, " +
					"UserName) " +
					"VALUES (" + "#" + this.m_dateTimeCall.ToString("yyyy/MM/dd HH:mm:ss") + "#, " +
					"'" + this.m_strCellLocation + "', " +
					"'" + this.m_strHouseBlockName + "', " +
					"'" + this.m_strUserId + "', " +
					"'" + this.m_strUserName + "')";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				int Result = cmd.ExecuteNonQuery();
				cmd.Dispose();
				db.Close();
				Logger.LogDbgString(Result + " row(s) inserted");
			}
			catch ( Exception e) 
			{
				Logger.LogString("Error inserting row into AttendanceLog table");
				Logger.LogString("Error message is: " + e.Message );
				throw;
			}
			catch 
			{
				Logger.LogString("Unknown exception inserting row into AttendanceLog table");
				throw;
			}
		}


		public static void PurgeLogData(DateTime deleteBefore)
		{
			try
			{
				PollerDB db = new PollerDB();

				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText  =	" SELECT FileLocation " + 
					" FROM CallLog " + 
					" WHERE CallDateTime < #" + deleteBefore.ToString("yyyy/MM/dd 00:00:00") + "#";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				OdbcDataReader MyReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

				while( MyReader.Read() )
				{
					string strFileToDelete = MyReader.GetString(0);

                    FileInfo fi = new FileInfo(strFileToDelete);

					try
					{
    				    if (fi.Exists)
                        {
							//fi.Delete();
							//Logger.LogInfoString("Purged file " + fi.FullName);
                            
                            // All files in this directory must go
			                DirectoryInfo di = new DirectoryInfo(fi.DirectoryName);
                            FileInfo[] files = di.GetFiles();

                            foreach (System.IO.FileInfo fi2 in files)
                            {
                                try
                                {
                                    fi2.Delete();
                                    Logger.LogInfoString("Purged file " + fi2.FullName);
                                }
                                catch
                                {
                                    Logger.LogInfoString("Couldn't purge file " + fi2.FullName);
									throw;
                                }
                            }
                            di.Delete();
                        }
					}
					catch (Exception e)
					{
						Logger.LogInfoString("Error purging files in " + fi.DirectoryName);
						Logger.LogString("Message is " + e.Message);
						throw;
					}
					catch
					{
						Logger.LogString("Unknown Exception while purging files in " + fi.DirectoryName);
						throw;
					}
				}

				MyReader.Close();
				cmd.Dispose();
				db.Close();

                // Now delete records from db
                try
                {
                    PollerDB db2 = new PollerDB();

                    System.Data.Odbc.OdbcCommand cmd2 = new OdbcCommand();
                    cmd2.Connection = db2.Connection;
                    cmd2.CommandText = "DELETE FROM CallLog WHERE " +
                        "CallDateTime < #" + deleteBefore.ToString("yyyy/MM/dd 00:00:00") + "#";
                    cmd2.CommandType = CommandType.Text;
                    Logger.LogDbgString(cmd2.CommandText);
                    int Result = cmd2.ExecuteNonQuery();
                    Logger.LogInfoString(Result + " row(s) purged from CallLog table");
                    cmd2.Dispose();
                    db2.Close();
                }
                catch ( Exception e) 
                {
                    Logger.LogString("Error purging from CallLog table");
                    Logger.LogString("Error message is: " + e.Message );
					throw;
                }
                catch 
                {
                    Logger.LogString("Unknown exception purging from CallLog table");
					throw;
                }

			}
			catch ( Exception e) 
			{
				Logger.LogString("Error purging from CallLog table/files");
				Logger.LogString("Error message is: " + e.Message );
			}
			catch 
			{
				Logger.LogString("Unknown exception purging from CallLog table/files");
			}
		}


		public static void PurgeAttendanceData(DateTime deleteBefore)
		{
			try
			{
				PollerDB db = new PollerDB();

				System.Data.Odbc.OdbcCommand cmd = new OdbcCommand();
				cmd.Connection = db.Connection;
				cmd.CommandText = "DELETE FROM AttendanceLog WHERE " +
					"CallDateTime < #" + deleteBefore.ToString("yyyy/MM/dd 00:00:00") + "#";
				cmd.CommandType = CommandType.Text;
				Logger.LogDbgString(cmd.CommandText);
				int Result = cmd.ExecuteNonQuery();
				Logger.LogInfoString(Result + " row(s) purged from AttendanceLog table");
				cmd.Dispose();
				db.Close();
			}
			catch ( Exception e) 
			{
				Logger.LogString("Error purging from AttendanceLog table");
				Logger.LogString("Error message is: " + e.Message );
			}
			catch 
			{
				Logger.LogString("Unknown exception purging from AttendanceLog table");
			}
		}
	}
}
