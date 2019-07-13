using Microsoft.SqlServer.TransactSql.ScriptDom;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using sqlparser.Modele;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sqlparser
{
    public class Parser
    {
        string pathResult = "";
        Chekable chekable = new Chekable();

        
        public Parser(string path)
        {
            pathResult = path;
            chekable.pathResult = pathResult;
        }

        public void ParserAll(string dic)
        {
            foreach (string file in Directory.EnumerateFiles(dic, "*.sql", SearchOption.AllDirectories))
            {
                Console.WriteLine("path {0}", file);
                this.outputPath = file;
                chekable.outputPath = outputPath;
                ParserFile();
            }
        }

        public void ParserFile()
        {
            TSql100Parser t = new TSql100Parser(true);

            //using (TextReader sr = new StringReader(""))
            //{
            //}
            using (StreamReader file = File.OpenText(outputPath))
            {
                //var ress = GetUsedTablesFromQuery(open.ReadToEnd());
                IList<ParseError> errors;
                TSqlFragment frag = t.Parse(file, out errors);
                StringBuilder errorsString = new StringBuilder();

                foreach (ParseError error in errors ?? new List<ParseError>())
                {
                    errorsString.AppendFormat("{0} {1}\r\n", error.Message, error.Line);
                    chekable.messages.addMessage(Code.T0000006, null, error.Message, error.Line.ToString());
                }

                var scipt = frag as TSqlScript;
                if (errorsString.Length > 0)
                {
                    SaveToFileParseError(errorsString.ToString());
                }
                if (scipt == null)
                {
                    file.Close();
                    return;
                }

                
                foreach (var item in scipt.Batches)
                {
                    //SqlScriptGeneratorOptions opt = new SqlScriptGeneratorOptions();
                    //Sql100ScriptGenerator gen = new Sql100ScriptGenerator();
                    //string str;
                    //gen.GenerateScript(item, out str);

                    //varible.Clear();
                    chekable.CheckStatments(item.Statements);


                    chekable.clearObjectFromBatche();
                    chekable.PostBatcheChecable();
                }
                chekable.clearObjectFromFile();
                chekable.PostFileChecable();

                foreach (var message in chekable.messages.Messages.OrderBy(c => c.Format?.StartLine))
                {
                    //switch (message.Text.Type)
                    //{
                    //    case TypeMessage.Warning:
                    //        Console.ForegroundColor = ConsoleColor.Yellow;
                    //        break;
                    //    case TypeMessage.Error:
                    //        Console.ForegroundColor = ConsoleColor.Red;
                    //        break;
                    //    case TypeMessage.Debug:
                    //        Console.ForegroundColor = ConsoleColor.Gray;
                    //        break;
                    //    default:
                    //        break;
                    //}
                    Console.WriteLine(message.MessageInformation);
                    Console.ResetColor();
                }
                file.Close();

                Console.ReadLine();
            }
        }

        
        public string outputPath { get; set; }
        private void SaveToFileParseError(string v)
        {
            string Name = "_ParseError_" + Path.GetFileName(this.outputPath);
            int i = 1;
            string FullPathResult = "";
            do
            {
                FullPathResult = Path.Combine(pathResult, Name + "_" + (i++) + ".json");
            } while (File.Exists(FullPathResult));

            File.WriteAllText(FullPathResult, this.outputPath + "\r\n\r\n" + v, Encoding.Default);
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
    }
}
