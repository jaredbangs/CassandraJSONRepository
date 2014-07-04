using System;

namespace AgileHub.CassandraJSONRepository
{
	public interface ILog
	{
		void Error(string text);
		void Information(string text);
		void Warning(string text);
	}

	public class NullLog : ILog
	{
		public void Error(string text) {}
		public void Information(string text) {}
		public void Warning(string text) {}
	}
}