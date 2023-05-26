using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLQueryLineage.Common
{
    public class ParserProperties
    {
        public string defaultSchema { get; set; } = "dbo";
        public string defaultDatabase { get; set; } = "master";
        public bool isLinkedServer { get; set; } = false;
        public bool compress { get; set; } = false;
    }
}
