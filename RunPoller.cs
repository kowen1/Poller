using System;

namespace Poller
{
	/// <summary>
	/// Summary description for RunPoller.
	/// </summary>
	public class RunPoller
	{
		public RunPoller()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		static void Main()
		{
			Logger.LogString("Poller version 2 Started in Non-Service Mode");
			Poller MyPoller = new Poller();
			MyPoller.Start();
			Console.ReadLine();
		}

	}
}


