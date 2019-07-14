using Microsoft.VisualStudio.TestTools.UnitTesting;
using sqlparser;
using sqlparser.Modele;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sqlparser.Tests
{
    [TestClass()]
    public class ParserTests
    {
        [TestMethod] public void Declare_nvarchar_length()
        {
            Parser parser = new Parser();
            parser.ParseSqlString("Declare @n nvarchar(1) = '123'");

            Assert.AreEqual(parser.Chekable.Messages.Count, 1);
            Assert.AreEqual(parser.Chekable.Messages[0].Code, nameof(Code.T0000002));
        }
        [TestMethod] public void Declare_varchar_length()
        {
            Parser parser = new Parser();
            parser.ParseSqlString("Declare @n varchar(1) = '123'");

            Assert.AreEqual(parser.Chekable.Messages.Count, 1);
            Assert.AreEqual(parser.Chekable.Messages[0].Code, nameof(Code.T0000002));
        }
        [TestMethod] public void Declare_nvarchar_select_length()
        {
            Parser parser = new Parser();
            parser.ParseSqlString("Declare @n nvarchar(1) = (select '123')");

            Assert.AreEqual(parser.Chekable.Messages.Count, 1);
            Assert.AreEqual(parser.Chekable.Messages[0].Code, nameof(Code.T0000002));
        }

        [TestMethod] public void Declare_nvarchar_select2()
        {
            Parser parser = new Parser();
            parser.ParseSqlString("Declare @n nvarchar(1) = (select '123','1234')");

            Assert.AreEqual(parser.Chekable.Messages.Count, 1);
            Assert.AreEqual(parser.Chekable.Messages[0].Code, nameof(Code.T0000012));
        }

        [TestMethod] public void Declare_nvarchar_select21()
        {
            Parser parser = new Parser();
            parser.ParseSqlString("Declare @n nvarchar(1) = (select '123','1234')");

            Assert.AreEqual(parser.Chekable.Messages.Count, 1);
            Assert.AreEqual(parser.Chekable.Messages[0].Code, nameof(Code.T0000012));
        }

        [TestMethod]
        public void Declare_nvarchar_value_int()
        {
            Parser parser = new Parser();
            parser.ParseSqlString("Declare @n nvarchar(1) = 1");

            Assert.AreEqual(parser.Chekable.Messages.Count, 1);
            Assert.AreEqual(parser.Chekable.Messages[0].Code, nameof(Code.T0000047));
        }
        [TestMethod]
        public void Declare_nvarchar_value_int_convert_nvarchar()
        {
            Parser parser = new Parser();
            parser.ParseSqlString("DECLARE @n NVARCHAR(1) = CAST(9 AS NVARCHAR(1))");

            Assert.AreEqual(parser.Chekable.Messages.Count, 1);
            Assert.AreEqual(parser.Chekable.Messages[0].Code, nameof(Code.T0000047));
        }

        //[TestMethod] public void Where_nvarchar_int()
        //{
        //    Parser parser = new Parser();
        //    parser.ParseSqlString("select * from t where 1 = '1'");

        //    Assert.AreEqual(parser.Chekable.Messages.Count, 1);
        //    Assert.AreEqual(parser.Chekable.Messages[0].Code, nameof(Code.T0000012));
        //}
    }
}