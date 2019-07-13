using System.Collections.Generic;

namespace sqlparser
{
    public class Table : ObjectFromServer
    {
        public Table()
        {
            IsExists = false;
            Columns = new List<MyColumn>();
        }
        public Table(string name) : this()
        {
            Name = name;
        }
        public bool IsExists { get; set; }
        string name;
        public string Name { get { return name; } set { name = value; IsExists = true; } }
        public List<MyColumn> Columns { get; set; }
    }
}
