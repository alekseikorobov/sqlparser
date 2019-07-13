using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;

namespace sqlparser
{
    public class Message
    {
        public Message()
        {
            Messages = new List<Message>();
        }
        public void addMessage(string code, TSqlFragment format, params string[] data)
        {
            Messages.Add(new Message(code, data, format));
        }
        //public void addMessage(string text, TSqlFragment format, params string[] data)
        //{
        //    Messages.Add(new Message(text, data, format));
        //}
        public List<Message> Messages { get; set; }

        public Message(string code, string[] data, TSqlFragment format)
        {
            this.Code = code;
            this.Data = data;
            this.Format = format;
        }
        //public Message(MyTyps text, string[] data, TSqlFragment format)
        //{
        //    this.text = text;
        //    this.Data = data;
        //    this.Format = format;
        //}
        public string MessageInformation
        {
            get
            {
                //string template = string.Format("({0}) {1} Line: {2}", Code, Text.Message,Format?.StartLine);
                return string.Format(Code, Data);
            }
        }
        public string Code { get; set; }
        public string[] Data { get; set; }

        public TSqlFragment Format { get; set; }
    }
}
