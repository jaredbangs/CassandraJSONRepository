using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra;

namespace AgileHub.CassandraJSONRepository
{
    public class Repository<TValue> : Repository<Guid,TValue>
    {
		public Repository(IJSONSerializer serializer, string nodeAddress, string keyspaceName, ILog log) : base(serializer, nodeAddress, keyspaceName, log)
        {
        }
    }

    public class Repository<TKey,TValue> : IDisposable
    {
        private readonly Cluster cluster;
        private readonly string keyspaceName;
		private readonly ILog log;
		private readonly int replicationFactor = 2;
        private readonly IJSONSerializer serializer; 
        private readonly string tableName;

		public Repository(IJSONSerializer serializer, string nodeAddress, string keyspaceName) : 
			this(serializer, nodeAddress, keyspaceName, new NullLog())
		{
		}

		public Repository(IJSONSerializer serializer, string nodeAddress, string keyspaceName, ILog log)
        {
			this.log = log;

            this.serializer = serializer;

            this.keyspaceName = keyspaceName;

            this.tableName = typeof(TValue).Name;

            this.cluster = Cluster.Builder().AddContactPoint(nodeAddress).Build();

            using (ISession session = cluster.Connect())
            {
				this.log.Information ("Repository setup started");
                session.Execute(
                    "CREATE KEYSPACE IF NOT EXISTS " + keyspaceName + 
                    " WITH replication = {'class':'SimpleStrategy', 'replication_factor':" + replicationFactor + "};");

                session.Execute(
                      "CREATE TABLE IF NOT EXISTS " + keyspaceName +  "." + tableName +  " (id " + this.GetCassandraIdType() + " PRIMARY KEY, json text);");
				this.log.Information ("Repository setup ended");
            }
        }

        public void Delete(TKey key)
        {
			this.log.Information ("Delete session starting");
            using (ISession session = cluster.Connect())
            {
                this.Delete(session, key);
            }
			this.log.Information ("Delete session ending");
        }

        public void Delete(ISession session, TKey key)
        {
			this.log.Information ("Delete starting");
            var preparedStatement = session.Prepare("DELETE FROM " + keyspaceName +  "." + tableName +  " WHERE id = ?;");

            var boundStatement = preparedStatement.Bind(key);

            session.Execute(boundStatement);
			this.log.Information ("Delete ending");
        }

        public void DeleteAll()
        {
			this.log.Information ("DeleteAll session starting");
            using (ISession session = cluster.Connect())
            {
                this.DeleteAll(session);
            }
			this.log.Information ("DeleteAll session ending");
        }

        public void DeleteAll(ISession session)
        {
			this.log.Information ("DeleteAll starting");
            var preparedStatement = session.Prepare("TRUNCATE " + keyspaceName + "." + tableName + ";");

            var boundStatement = preparedStatement.Bind();

            session.Execute(boundStatement);
			this.log.Information ("DeleteAll ending");
        }

        public void Dispose()
        {
			this.log.Information ("Dispose starting");
            this.cluster.Shutdown();
            this.cluster.Dispose();
			this.log.Information ("Dispose ending");
        }

        public TValue Get(TKey key)
        {
			this.log.Information ("Get session starting");
            using (ISession session = cluster.Connect())
            {
				this.log.Information ("Get session ending");
                return this.Get(session, key);
            }
        }

        public TValue Get(ISession session, TKey key)
        {
			this.log.Information ("Get starting");
            var preparedStatement = session.Prepare("SELECT json FROM " + keyspaceName + "." + tableName + " WHERE id = ?;");

            var boundStatement = preparedStatement.Bind(key);

            RowSet results = session.Execute(boundStatement);

            if (!results.IsExhausted())
            {
				this.log.Information ("Get returning value");
                return this.serializer.DeserializeObject<TValue>(results.GetRows().First().GetValue<string>("json"));
            } 
            else
            {
				this.log.Information ("Get returning no value");
                return default(TValue);
            }
        }

        public IEnumerable<TValue> GetAll()
        {
			this.log.Information ("GetAll session starting");
            using (ISession session = cluster.Connect())
            {
                foreach(var item in this.GetAll(session)) 
                {
                    yield return item;
                }
				this.log.Information ("GetAll session ending");
            }
        }

        public IEnumerable<TValue> GetAll(ISession session)
        {
			this.log.Information ("GetAll starting");
            var preparedStatement = session.Prepare("SELECT json FROM " + keyspaceName + "." + tableName + ";");

            var boundStatement = preparedStatement.Bind();

            RowSet results = session.Execute(boundStatement);

            if (!results.IsExhausted())
            {
				this.log.Information ("GetAll returning values");
                foreach(var row in results.GetRows())
                {
                    yield return this.serializer.DeserializeObject<TValue>(row.GetValue<string>("json"));
                }
            } 
            else
            {
				this.log.Information ("GetAll returning no values");
                yield return default(TValue);
            }
        }

        public ISession NewSession()
        {
            return cluster.Connect();
        }

        public void Save(TKey key, TValue item)
        {
			this.log.Information ("Save session starting");
            using (ISession session = cluster.Connect())
            {
                this.Save(session, key, item);
            }
			this.log.Information ("Save session ending");
        }

        public void Save(ISession session, TKey key, TValue item)
        {
			this.log.Information ("Save starting");

			string json = this.serializer.SerializeObject(item);

            var preparedStatement = session.Prepare("UPDATE " + keyspaceName +  "." + tableName +  " SET json = ? WHERE id = ?;");

            var boundStatement = preparedStatement.Bind(json, key);

            session.Execute(boundStatement);

			this.log.Information ("Save ending");
        }

        private string GetCassandraIdType() 
        {
            if(typeof(TKey) == typeof(Guid)) return "uuid";
            if(typeof(TKey) == typeof(string)) return "varchar";
            if(typeof(TKey) == typeof(long)) return "bigint";

            throw new Exception("Unsupported Id Type");
        }
    }
}