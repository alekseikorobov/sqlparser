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
        public Parser(string path)
        {
            pathResult = path;
        }

        public void ParserAll(string dic)
        {
            foreach (string file in Directory.EnumerateFiles(dic, "*.sql", SearchOption.AllDirectories))
            {
                Console.WriteLine("path {0}", file);
                this.outputPath = file;
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
                    CheckStatment(item.Statements);
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

        private void CheckStatment(IList<TSqlStatement> statements)
        {
            foreach (TSqlStatement statement in statements)
            {
                if (statement == null) continue;

                switch (statement)
                {
                    case CreateTableStatement c: chekable.FullCheck(c); break;
                    case CreateProcedureStatement c: chekable.FullCheck(c); break;
                    default:
                        break;
                }
                if (statement is CreateTableStatement)
                {
                    chekable.getCreateTableStatement(statement as CreateTableStatement);
                }
                else
                if (statement is CreateProcedureStatement)
                {
                    chekable.getCreateProcedureStatement(statement as CreateProcedureStatement);
                    CheckStatment((statement as CreateProcedureStatement).StatementList.Statements);
                    chekable.PostAllStatmentChecable();
                    chekable.clearObjectFromStatement();
                }
                else
                if (statement is AlterProcedureStatement)
                {
                    chekable.getAlterProcedureStatement(statement as AlterProcedureStatement);
                    CheckStatment((statement as AlterProcedureStatement).StatementList.Statements);
                    chekable.PostAllStatmentChecable();
                    chekable.clearObjectFromStatement();
                }
                else
                if (statement is WhileStatement)
                {
                    CheckStatment(new[] { (statement as WhileStatement).Statement });
                }
                else
                if (statement is IfStatement)
                {
                    var ifStatement = statement as IfStatement;

                    if (ifStatement.Predicate is BooleanExpression)
                    {
                        chekable.checkedBooleanComparison(ifStatement.Predicate as BooleanExpression);
                    }

                    CheckStatment(new[] { ifStatement.ThenStatement });
                    CheckStatment(new[] { ifStatement.ElseStatement });
                }
                else
                if (statement is BeginEndBlockStatement)
                {
                    CheckStatment((statement as BeginEndBlockStatement).StatementList.Statements);
                }
                else
                if (statement is ProcedureStatementBodyBase)
                {
                    CheckStatment((statement as ProcedureStatementBodyBase).StatementList.Statements);
                }
                else
                if (statement is SetVariableStatement)
                {
                    chekable.getSetVariableStatement(statement as SetVariableStatement);
                }
                else
                if (statement is DeclareVariableStatement)
                {
                    chekable.getDeclareVariableStatement(statement as DeclareVariableStatement);
                }
                else
                if (statement is DeclareTableVariableStatement)
                {
                    chekable.getDeclareTableVariableStatement(statement as DeclareTableVariableStatement);
                }
                else
                if (statement is SelectStatement)
                {
                    //SaveToFile(path, statement);
                    chekable.getSelectStatement(statement as SelectStatement);
                }
                else
                if (statement is InsertStatement)
                {
                    chekable.getInsertStatement(statement as InsertStatement);
                }
                else
                if (statement is DropTableStatement)
                {
                    chekable.getDropTableStatement(statement as DropTableStatement);
                }
                else
                {
                    SaveToFile(statement);
                }

                //chekable.clearObjectFromStatement();
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

        void SaveToFile(TSqlStatement statement)
        {
            string sereal = "";
            var type = statement.GetType();
            string name = type.Name;
            try
            {
                var j = JObject.FromObject(statement);
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
                name = "_Exception_" + name;
            }
            int i = 1;
            string FullPathResult = "";
            do
            {
                FullPathResult = Path.Combine(pathResult, name + "_" + (i++) + ".json");
            } while (File.Exists(FullPathResult));

            File.WriteAllText(FullPathResult, outputPath + "\r\n\r\n" + sereal, Encoding.Default);
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
    }
}
