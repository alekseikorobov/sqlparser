using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace sqlparser
{
    [Serializable]
    internal class ExceptionTSqlFragment : Exception
    {
        private TSqlFragment tSqlFragment;

        public ExceptionTSqlFragment()
        {
        }

        public ExceptionTSqlFragment(TSqlFragment tSqlFragment)
        {
            this.tSqlFragment = tSqlFragment;
        }

        public ExceptionTSqlFragment(string message) : base(message)
        {
        }

        public ExceptionTSqlFragment(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected ExceptionTSqlFragment(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        #region SaveToFileIfNewStatement
        Dictionary<string, JTokenType> ondeleteNode = new Dictionary<string, JTokenType>()
        {
             {"ScriptTokenStream", JTokenType.Array }
            ,{"StartOffset", JTokenType.Integer }
            ,{"FragmentLength", JTokenType.Integer }
            ,{"StartLine", JTokenType.Integer }
            ,{"StartColumn", JTokenType.Integer }
            ,{"LastTokenIndex", JTokenType.Integer }
            ,{"FirstTokenIndex", JTokenType.Integer }
        };
        public string NameFile
        {
            get
            {
                var type = tSqlFragment.GetType();
                return type.FullName;
            }
        }
        public void SaveToFile(string inputSqlFile, string pathResult)
        {
            string sereal = GetJsonString();
            int i = 1;
            string FullPathResult = "";
            do
            {
                FullPathResult = Path.Combine(pathResult, NameFile + "_" + (i++) + ".json");
            } while (File.Exists(FullPathResult));

            File.WriteAllText(FullPathResult, inputSqlFile + "\r\n\r\n" + sereal, Encoding.Default);
        }

        public string GetJsonString()
        {
            string sereal;
            try
            {
                var j = JObject.FromObject(tSqlFragment);
                foreach (var item in ondeleteNode)
                {
                    j[item.Key].Parent.Remove();
                }
                removeall(j);

                JsonSerializerSettings setting = new JsonSerializerSettings();
                sereal = j.ToString();
            }
            catch (Exception ex)
            {
                sereal = string.Format("Exception - {0}\r\n\r\n StackTrace - {1}", ex.Message, ex.StackTrace);
                //NameFile = "_Exception_" + NameFile;
            }
            var type = tSqlFragment.GetType();
            return sereal;
        }

        private void removeall(JObject j)
        {
            foreach (var node in j.Values())
            {
                WalkNode(node, n =>
                {


                    foreach (var item in ondeleteNode)
                    {
                        JToken token = n[item.Key];
                        if (token != null && token.Type == item.Value)
                        {
                            token.Parent.Remove();
                        }
                    }
                });
            }
        }
        void WalkNode(JToken node, Action<JObject> action)
        {
            if (node.Type == JTokenType.Object)
            {
                action((JObject)node);

                foreach (JProperty child in node.Children<JProperty>())
                {
                    WalkNode(child.Value, action);
                }
            }
            else if (node.Type == JTokenType.Array)
            {
                foreach (JToken child in node.Children())
                {
                    WalkNode(child, action);
                }
            }
        }
        #endregion
    }
}