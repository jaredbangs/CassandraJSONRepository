using System;

namespace AgileHub.CassandraJSONRepository.Tests
{
    public class SampleObject
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public DateTime Start { get; set; }
    }

    public class SampleStringIdObject
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public DateTime Start { get; set; }
    }

}

