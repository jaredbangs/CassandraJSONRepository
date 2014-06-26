using System;
using System.Collections.Generic;
using System.Linq;
using Should;

namespace AgileHub.CassandraJSONRepository.Tests
{
    public class RepositoryTests
    {
        private readonly string keyspaceName = "CassandraJSONRepositoryTests";
        private readonly string nodeAddress = "192.168.8.124";
        private readonly Random random = new Random();
        private readonly IJSONSerializer serializer = new JSONSerializer(); 

        public void GetAllTest()
        {
            int iterations = 50;

            var savedObjects = new Dictionary<Guid,SampleObject>();

            using (var target = new Repository<SampleObject>(serializer, nodeAddress, keyspaceName))
            {
                target.DeleteAll();

                for(int i = 0; i < iterations; i++)
                {
                    var testObject = new SampleObject { Id = Guid.NewGuid(), Name = "Testing" + random.Next(), Start = DateTime.UtcNow };

                    target.Save(testObject.Id, testObject);

                    savedObjects.Add(testObject.Id, testObject);
                }
            }

            using (var target = new Repository<SampleObject>(serializer, nodeAddress, keyspaceName))
            {
                IEnumerable<SampleObject> all = target.GetAll();

                all.Count().ShouldEqual(iterations);

                foreach(Guid key in savedObjects.Keys) 
                {
                    target.Delete(key);
                }
            }
        }


        public void GuidSaveAndGetTest()
        {
            var testObject = new SampleObject { Id = Guid.NewGuid(), Name = "Testing" + random.Next(), Start = DateTime.UtcNow };

            using (var target = new Repository<SampleObject>(serializer, nodeAddress, keyspaceName))
            {
                target.Save(testObject.Id, testObject);
            }

            using (var target = new Repository<SampleObject>(serializer, nodeAddress, keyspaceName))
            {
                var reloaded = target.Get(testObject.Id);

                reloaded.Name.ShouldEqual(testObject.Name);
                reloaded.Start.ShouldEqual(testObject.Start);
            }
        }

        public void GuidSaveGetAndDeleteTest()
        {
            var testObject = new SampleObject { Id = Guid.NewGuid(), Name = "Testing" + random.Next(), Start = DateTime.UtcNow };

            using (var target = new Repository<SampleObject>(serializer, nodeAddress, keyspaceName))
            {
                target.Save(testObject.Id, testObject);
            }

            using (var target = new Repository<SampleObject>(serializer, nodeAddress, keyspaceName))
            {
                var reloaded = target.Get(testObject.Id);
                reloaded.Name.ShouldEqual(testObject.Name);
                reloaded.Start.ShouldEqual(testObject.Start);
            }

            using (var target = new Repository<SampleObject>(serializer, nodeAddress, keyspaceName))
            {
                target.Delete(testObject.Id);
            }

            using (var target = new Repository<SampleObject>(serializer, nodeAddress, keyspaceName))
            {
                var reloaded = target.Get(testObject.Id);
                reloaded.ShouldBeNull();
            }

        }

        public void StringSaveAndGetTest()
        {
            var testObject = new SampleStringIdObject { Id = Guid.NewGuid().ToString(), Name = "Testing" + random.Next(), Start = DateTime.UtcNow };

            using (var target = new Repository<string,SampleStringIdObject>(serializer, nodeAddress, keyspaceName))
            {
                target.Save(testObject.Id, testObject);
            }

            using (var target = new Repository<string,SampleStringIdObject>(serializer, nodeAddress, keyspaceName))
            {
                var reloaded = target.Get(testObject.Id);

                reloaded.Name.ShouldEqual(testObject.Name);
                reloaded.Start.ShouldEqual(testObject.Start);
            }
        }
    }
}

