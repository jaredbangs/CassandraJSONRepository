using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra;

namespace AgileHub.CassandraJSONRepository
{
    public class Repository<TValue> : Repository<Guid,TValue>
    {
        public Repository(IJSONSerializer serializer, string nodeAddress, string keyspaceName) : base(serializer, nodeAddress, keyspaceName)
        {
        }
    }

    public class Repository<TKey,TValue> : IDisposable
    {
        private readonly Cluster cluster;
        private readonly string keyspaceName;
        private readonly int replicationFactor = 2;
        private readonly IJSONSerializer serializer; 
        private readonly string tableName;

        public Repository(IJSONSerializer serializer, string nodeAddress, string keyspaceName)
        {
            this.serializer = serializer;

            this.keyspaceName = keyspaceName;

            this.tableName = typeof(TValue).Name;

            this.cluster = Cluster.Builder().AddContactPoint(nodeAddress).Build();

            using (ISession session = cluster.Connect())
            {
                session.Execute(
                    "CREATE KEYSPACE IF NOT EXISTS " + keyspaceName + 
                    " WITH replication = {'class':'SimpleStrategy', 'replication_factor':" + replicationFactor + "};");

                session.Execute(
                      "CREATE TABLE IF NOT EXISTS " + keyspaceName +  "." + tableName +  " (id " + this.GetCassandraIdType() + " PRIMARY KEY, json text);");
            }
        }

        public void Delete(TKey key)
        {
            using (ISession session = cluster.Connect())
            {
                this.Delete(session, key);
            }
        }

        public void Delete(ISession session, TKey key)
        {
            var preparedStatement = session.Prepare("DELETE FROM " + keyspaceName +  "." + tableName +  " WHERE id = ?;");

            var boundStatement = preparedStatement.Bind(key);

            session.Execute(boundStatement);
        }

        public void DeleteAll()
        {
            using (ISession session = cluster.Connect())
            {
                this.DeleteAll(session);
            }
        }

        public void DeleteAll(ISession session)
        {
            var preparedStatement = session.Prepare("TRUNCATE " + keyspaceName + "." + tableName + ";");

            var boundStatement = preparedStatement.Bind();

            session.Execute(boundStatement);
        }

        public void Dispose()
        {
            this.cluster.Shutdown();
            this.cluster.Dispose();
        }

        public TValue Get(TKey key)
        {
            using (ISession session = cluster.Connect())
            {
                return this.Get(session, key);
            }
        }

        public TValue Get(ISession session, TKey key)
        {
            var preparedStatement = session.Prepare("SELECT json FROM " + keyspaceName + "." + tableName + " WHERE id = ?;");

            var boundStatement = preparedStatement.Bind(key);

            RowSet results = session.Execute(boundStatement);

            if (!results.IsExhausted())
            {
                return this.serializer.DeserializeObject<TValue>(results.GetRows().First().GetValue<string>("json"));
            } 
            else
            {
                return default(TValue);
            }
        }

        public IEnumerable<TValue> GetAll()
        {
            using (ISession session = cluster.Connect())
            {
                foreach(var item in this.GetAll(session)) 
                {
                    yield return item;
                }
            }
        }

        public IEnumerable<TValue> GetAll(ISession session)
        {
            var preparedStatement = session.Prepare("SELECT json FROM " + keyspaceName + "." + tableName + ";");

            var boundStatement = preparedStatement.Bind();

            RowSet results = session.Execute(boundStatement);

            if (!results.IsExhausted())
            {
                foreach(var row in results.GetRows())
                {
                    yield return this.serializer.DeserializeObject<TValue>(row.GetValue<string>("json"));
                }
            } 
            else
            {
                yield return default(TValue);
            }
        }

        public void Save(TKey key, TValue item)
        {
            using (ISession session = cluster.Connect())
            {
                this.Save(session, key, item);
            }
        }

        public void Save(ISession session, TKey key, TValue item)
        {
            string json = this.serializer.SerializeObject(item);

            var preparedStatement = session.Prepare("UPDATE " + keyspaceName +  "." + tableName +  " SET json = ? WHERE id = ?;");

            var boundStatement = preparedStatement.Bind(json, key);

            session.Execute(boundStatement);
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