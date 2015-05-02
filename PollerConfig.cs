using System;
using System.Configuration;

namespace Poller
{
	/// <summary>
	/// 
	/// </summary>
	public class PollerConfig
	{
		public static int m_diffSecs = Convert.ToInt16(ConfigurationSettings.AppSettings["DiffSeconds"]);
		public static int m_daysInDb = Convert.ToInt16(ConfigurationSettings.AppSettings["DaysInDB"]);
		public static int m_daysInLocation = Convert.ToInt16(ConfigurationSettings.AppSettings["DaysInLocation"]);
		public static string m_strMoveToDir = ConfigurationSettings.AppSettings["MoveToDir"];
		public static string m_strMoveToErrorDir = ConfigurationSettings.AppSettings["MoveToErrorDir"];
		public static int m_logMode = Convert.ToInt16(ConfigurationSettings.AppSettings["LogMode"]);
		public static string m_strLogFile = ConfigurationSettings.AppSettings["LogFile"];
		public static string m_sConnStr = ConfigurationSettings.AppSettings["PollerConnection"];
		public static long m_LogFileSize = 1024 * Convert.ToInt32(ConfigurationSettings.AppSettings["LogFileSizeInKB"]);

		static PollerConfig()
		{
			// 
			// TODO: Add constructor logic here
			//
		}
	}
}
