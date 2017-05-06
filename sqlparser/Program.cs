using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.IO;
using System.Collections;
using System.Runtime.Serialization.Formatters.Binary;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

/// <summary>
/// Валидация:
/// проверка переменных
/// 
/// Проверка существования таблиц по указанному коннекшену, проверка регистра таблиц (опционально)
/// Проверка существования полей и их регистр (опционально)
/// 
/// проверка обращения алиаса и поля
/// 
/// проверка update, delete, insert
/// </summary>

namespace sqlparser
{
    class Program
    {
        private static string pathresult = @"C:\Users\akorobov\Documents\Visual Studio 2015\Projects\sqlparser\sqlparser\types";

        static void Main(string[] args)
        {
            //string dic = Environment.CurrentDirectory;
            try
            {
                string dic = @"c:\share\data\SQL\";
                var p = new Parser(pathresult);
                //p.ParserAll(dic);
                //Parser(@"c:\share\data\SQL\sqlWork\запросы лабы.sql");
                //Parser(@"c:\share\data\SQL\_SQL\_SQL\my project\COD\Актуальные процедуры.sql");

                p.ParserFile(@"script.sql");

                //p.ParserFile(@"c:\share\data\SQL\_SQL\_SQL\my project\newProcedure_Compare.sql");

            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Message-{0}; StackTrace - {1};", ex.Message, ex.StackTrace);
                Console.ResetColor();
                Console.ReadLine();
            }
        }
    }

}
