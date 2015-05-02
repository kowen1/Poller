using System;
using System.IO;
using System.Configuration;

namespace Poller
{
	/// <summary>
	/// Summary description for Logger.
	/// </summary>
	public class Logger
	{
		private static System.IO.StreamWriter w;

		static Logger()
		{
			w = new System.IO.StreamWriter(PollerConfig.m_strLogFile, true);
			LogString("Log Created - build 02/12/03 21:43:00");
		}

		~Logger()
		{
            // Add a comment here
			w.Close();
		}

		public static void LogString(string String)
		{
			// Don't log if logMode < 1
			if (PollerConfig.m_logMode < 1)
				return;

			try
			{
				CheckLogFileSize();
				Console.WriteLine(String);
				w.WriteLine(System.DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss.ff") + " " + String);
				w.Flush();
			}
			catch
			{
			}
		}


		public static void LogDbgString(string String)
		{
			// Don't log if logMode < 3
			if (PollerConfig.m_logMode < 3)
				return;

			try
			{
				CheckLogFileSize();
				Console.WriteLine(String);
				w.WriteLine(System.DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss.ff") + " " + String);
				w.Flush();
			}
			catch
			{
			}
		}

		public static void LogInfoString(string String)
		{
			// Don't log if logMode < 2
			if (PollerConfig.m_logMode < 2)
				return;

			try
			{
				CheckLogFileSize();
				Console.WriteLine(String);
				w.WriteLine(System.DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss.ff") + " " + String);
				w.Flush();
			}
			catch
			{
			}
		}

		public static void CheckLogFileSize()
		{
			FileInfo fi = new FileInfo(PollerConfig.m_strLogFile);

			if (fi.Length > PollerConfig.m_LogFileSize)
			{
				w.Close();
				if (File.Exists(PollerConfig.m_strLogFile + ".old"))
					File.Delete(PollerConfig.m_strLogFile + ".old");
				else
				{
					File.Move(PollerConfig.m_strLogFile, PollerConfig.m_strLogFile + ".old");
					File.Delete(PollerConfig.m_strLogFile);
					w = new System.IO.StreamWriter(PollerConfig.m_strLogFile, true);
					LogString("New Logger File Created");
				}
			}
		}
	}
}
