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
        private string _pathResult = "";

        public string PathResult
        {
            get => _pathResult;
            set
            {
                _pathResult = value;
                //Chekable.PathResult = _pathResult;
            }
        }
        public Chekable Chekable { get; set; }


        public Parser()
        {
            Chekable = new Chekable();
        }

        public void ParserAll(string dic)
        {
            foreach (string file in Directory.EnumerateFiles(dic, "*.sql", SearchOption.AllDirectories))
            {
                Console.WriteLine("path {0}", file);

                ParserFile(file);
            }
        }

        public void ParseSqlString(string sql)
        {
            using (TextReader sr = new StringReader(sql))
            {
                ParseStream(sr);
            }
        }
        void ParseStream(TextReader file)
        {
            TSql100Parser t = new TSql100Parser(true);
            //var ress = GetUsedTablesFromQuery(open.ReadToEnd());
            TSqlFragment frag = t.Parse(file, out IList<ParseError> errors);
            StringBuilder errorsString = new StringBuilder();

            foreach (ParseError error in errors ?? new List<ParseError>())
            {
                errorsString.AppendFormat("{0} {1}\r\n", error.Message, error.Line);
                Chekable.AddMessage(Code.T0000006, null, error.Message, error.Line.ToString());
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
                try
                {
                    Chekable.CheckStatments(item.Statements);
                }
                catch (ExceptionTSqlFragment ex)
                {
                    var json = ex.GetJsonString();
                    string str = $"{ex.NameFile}\r\n{json}";
                    Console.WriteLine(str);
                    //Console.WriteLine(ex.Message);
                    //ex.SaveToFile(this.inputSqlFile, PathResult);
                }

                Chekable.clearObjectFromBatche();
                Chekable.PostBatcheChecable();
            }
            Chekable.clearObjectFromFile();
            Chekable.PostFileChecable();

            foreach (var message in Chekable.Messages.OrderBy(c => c.Format?.StartLine))
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
        }

        public void ParserFile(string inputSqlFile)
        {
            this.inputSqlFile = inputSqlFile;
            //Chekable.outputPath = outputPath;
            using (StreamReader inputSqlFileStream = File.OpenText(this.inputSqlFile))
            {
                this.ParseStream(inputSqlFileStream);
            }
            Console.ReadLine();
        }


        public string inputSqlFile { get; set; }
        private void SaveToFileParseError(string v)
        {
            string Name = "_ParseError_" + Path.GetFileName(this.inputSqlFile);
            int i = 1;
            string FullPathResult = "";
            do
            {
                FullPathResult = Path.Combine(PathResult, Name + "_" + (i++) + ".json");
            } while (File.Exists(FullPathResult));

            File.WriteAllText(FullPathResult, this.inputSqlFile + "\r\n\r\n" + v, Encoding.Default);
        }
    }
}
