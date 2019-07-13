using Microsoft.SqlServer.Management.Smo;

namespace sqlparser
{
    public class MyColumn : ObjectFromServer
    {
        public MyColumn(Column column)
        {
            this.Column = column;
        }
        public string FullName
        {
            get
            {
                return !string.IsNullOrEmpty(Alias) ? Alias + "." + Name : Name;
            }
        }
        public Column Column { get; set; }
        public string Alias { get; set; }
        public string Name { get; set; }
        public bool IsValid { get; internal set; }
    }
}
