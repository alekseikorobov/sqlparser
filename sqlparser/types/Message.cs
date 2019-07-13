using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;

namespace sqlparser
{
    public class Message
    {
        public Message()
        {
        }

        public Message(string value, string[] data, TSqlFragment format)
        {
            this.Value = value ?? throw new System.ArgumentNullException(nameof(value));
            this.Data = data;
            this.Format = format;
            this.Code = FindCodeByValue(value);
        }

        private string FindCodeByValue(string value)
        {
            var t = typeof(sqlparser.Modele.Code);
            var ps = t.GetProperties();
            foreach (System.Reflection.PropertyInfo propertyInfo in ps)
            {
                var val = propertyInfo.GetValue(t);
                if (val.Equals(value))
                {
                    return propertyInfo.Name;
                }
            }
            return "";
        }

        public string MessageInformation
        {
            get
            {
                return string.Format(Value, Data);
            }
        }
        public string Code { get; set; }
        public string Value { get; set; }
        public string[] Data { get; set; }

        public TSqlFragment Format { get; set; }
    }
}
