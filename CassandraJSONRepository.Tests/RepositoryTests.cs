using System;
using Should;

namespace CassandraJSONRepository.Tests
{
    public class RepositoryTests
    {
        private readonly string keyspaceName = "CassandraJSONRepositoryTests";
        private readonly string nodeAddress = "192.168.8.124";
        private readonly Random random = new Random();
        private readonly IJSONSerializer serializer = new JSONSerializer(); 

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

