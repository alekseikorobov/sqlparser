using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using sqlparser.Modele;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sqlparser
{
    public class Chekable
    {
        ObjectFromServer GetObjectFromServer(TSqlFragment type)
        {
            if (type is NamedTableReference)
            {
                var t = type as NamedTableReference;
                string fullNameTable = getNameTable(t);
                if (TableFromServer.ContainsKey(fullNameTable))
                {
                    return TableFromServer[fullNameTable];
                }
                var table = t.SchemaObject.BaseIdentifier.Value;
                var schema = t.SchemaObject.SchemaIdentifier?.Value ?? this.schema;
                database = t.SchemaObject.DatabaseIdentifier?.Value ?? database;
                Database db = server.Databases[database];
                SqlSmoObject obj = db.Tables[table, schema] as SqlSmoObject ?? db.Views[table, schema] as SqlSmoObject;
                var temptable = new Table();
                if (obj != null)
                {
                    if (obj is TableViewTableTypeBase)
                    {
                        var tableServer = obj as TableViewTableTypeBase;
                        temptable.Name = tableServer.ToString();
                        foreach (Column item in tableServer.Columns)
                        {
                            temptable.Columns.Add(new MyColumn(item));
                        }
                        TableFromServer.Add(fullNameTable, temptable);
                    }
                    else
                    {
                        AddMessage(Code.T0000022, type, fullNameTable, obj.GetType().Name);
                    }
                }
                else
                {
                    AddMessage(Code.T0000023, type, fullNameTable);
                    TableFromServer.Add(fullNameTable, temptable);
                }
                return temptable;
            }
            return null;
        }

        public Chekable()
        {
            server = new Server();
            server.ConnectionContext.ConnectionString = "data source=(local);initial catalog=master;integrated security=True;application name=master;MultipleActiveResultSets=True";

            bool isConnect = false;
            try
            {
                server.ConnectionContext.ConnectTimeout = 1;
                server.ConnectionContext.Connect();
                isConnect = true;
            }
            catch { }
            if (isConnect && server.ConnectionContext.IsOpen)
            {
                var conect = (server.ConnectionContext.SqlConnectionObject as SqlConnection);
                serverName = conect.WorkstationId;
                database = conect.Database;
                schema = server.Databases[database].DefaultSchema;
            }
            Messages = new List<Message>();
        }

        #region variable
        Dictionary<string, Table> TableFromServer = new Dictionary<string, Table>();
        string database = "";
        string schema = "dbo";
        string serverName = "RUMSKR90AZ5WD";
        bool IsAliasAll = true;

        public List<Message> Messages { get; set; }
        public void AddMessage(string value, TSqlFragment format, params string[] data)
        {
            Messages.Add(new Message(value, data, format));
        }

        Server server;
        Dictionary<string, ReferCount<DeclareVariableElement, int>> varible
            = new Dictionary<string, ReferCount<DeclareVariableElement, int>>();

        Dictionary<string, ReferCount<ProcedureParameter, int>> parametrs
            = new Dictionary<string, ReferCount<ProcedureParameter, int>>();

        Dictionary<string, ReferCount<SchemaObjectName, int>> dropTebles
            = new Dictionary<string, ReferCount<SchemaObjectName, int>>();

        Dictionary<string, ReferCount<BooleanIsNullExpression, int>> compareNull
            = new Dictionary<string, ReferCount<BooleanIsNullExpression, int>>();

        Dictionary<string, ReferCount<CreateTableStatement, int>> tempTeble
            = new Dictionary<string, ReferCount<CreateTableStatement, int>>();
        Dictionary<string, ReferCount<CreateTableStatement, int>> createTebles
            = new Dictionary<string, ReferCount<CreateTableStatement, int>>();

        Dictionary<string, ReferCount<DeclareTableVariableBody, int>> tableVarible
            = new Dictionary<string, ReferCount<DeclareTableVariableBody, int>>();

        Dictionary<string, ReferCount<TableReference, int>> tables
            = new Dictionary<string, ReferCount<TableReference, int>>();

        Dictionary<string, ReferCount<CommonTableExpression, int>> withTables
            = new Dictionary<string, ReferCount<CommonTableExpression, int>>();
        #endregion

        public void CheckUsengTableVarible()
        {
            foreach (var table in tableVarible)
            {
                if (table.Value.Count == 0)
                {
                    AddMessage(Code.T0000011, table.Value.Obj, table.Key);
                }
            }
        }
        public void CheckUsingVariable()
        {
            foreach (var value in varible)
            {
                if (value.Value.Count == 0)
                {
                    AddMessage(Code.T0000001, value.Value.Obj, value.Value.Obj.VariableName.Value);
                }
            }
        }
        public void CheckUsingParams()
        {
            foreach (var parametr in parametrs)
            {
                if (parametr.Value.Count == 0)
                {
                    AddMessage(Code.T0000045, parametr.Value.Obj, parametr.Value.Obj.VariableName.Value);
                }
            }
        }
        public void PostFileChecable()
        {
            ///проверка удаления временных таблиц            
        }
        public void PostAllStatmentChecable()
        {
            CheckUsingVariable();
            CheckUsingParams();
        }

        #region CheckStatmentRegion
        public void CheckStatment(CreateTableStatement createTableStatement)
        {
            var ident = createTableStatement.SchemaObjectName.BaseIdentifier.Value;
            if (IsTempTable(ident))
            {
                if (tempTeble.ContainsKey(ident))
                {
                    AddMessage(Code.T0000039, createTableStatement, ident);
                    return;
                }
                else
                {
                    tempTeble.Add(ident, new ReferCount<CreateTableStatement, int>(createTableStatement, 0));
                }
            }
            else
            {
                if (createTebles.ContainsKey(ident))
                {
                    AddMessage(Code.T0000040, createTableStatement, ident);
                    return;
                }
                else
                {
                    if (!dropTebles.ContainsKey(ident))
                    {
                        AddMessage(Code.T0000041, createTableStatement, ident);
                    }

                    createTebles.Add(ident, new ReferCount<CreateTableStatement, int>(createTableStatement, 0));
                }
            }
            //SchemaObjectName
        }
        public void CheckStatment(DeclareTableVariableStatement declareTableVar)
        {
            if (tableVarible.ContainsKey(declareTableVar.Body.VariableName.Value))
            {
                AddMessage(Code.T0000038, declareTableVar, declareTableVar.Body.VariableName.Value);
                return;
            }
            tableVarible.Add(declareTableVar.Body.VariableName.Value, new ReferCount<DeclareTableVariableBody, int>(declareTableVar.Body, 0));
        }
        public void CheckStatment(InsertStatement statement)
        {
            string target = "";
            bool isTargetValidate = false;
            List<ColumnDefinition> columns = new List<ColumnDefinition>();
            Table table = null;
            if (statement.InsertSpecification.Target is NamedTableReference)
            {
                var Target = statement.InsertSpecification.Target as NamedTableReference;
                CheckeTableReference(Target);
                target = getNameTable(Target);
                if (!IsTempTable(target))
                {
                    if (server.ConnectionContext.IsOpen)
                        table = GetObjectFromServer(Target) as Table;
                }
                //columns = getSpecificationTableTarget(Target);
                isTargetValidate = columns != null && columns.Count > 0;
            }
            if (statement.InsertSpecification.Target is VariableTableReference)
            {
                var body = getDeclareTableVariable(statement.InsertSpecification.Target as VariableTableReference);
                columns = body.Definition.ColumnDefinitions.ToList();
                target = body.VariableName.Value;
            }
            if (statement.InsertSpecification.Columns.Count == 0)
            {
                AddMessage(Code.T0000007, statement, target);
            }
            if (statement.InsertSpecification.InsertSource is SelectInsertSource)
            {
                var select = statement.InsertSpecification.InsertSource as SelectInsertSource;
                var query = select.Select as QuerySpecification;
                if (query.SelectElements.Count > statement.InsertSpecification.Columns.Count)
                {
                    AddMessage(Code.T0000009, statement, target);
                }
                else
                if (query.SelectElements.Count < statement.InsertSpecification.Columns.Count)
                {
                    AddMessage(Code.T0000010, statement, target);
                }
                getQuerySpecification(query);

                isTargetValidate = isTargetValidate && columns.Count > 0;
                for (int i = 0; i < query.SelectElements.Count; i++)
                {
                    var element = query.SelectElements[i];
                    if (isTargetValidate)
                    {
                        var column = columns[i];
                    }
                    if (element is SelectScalarExpression)
                    {
                        var expression = (element as SelectScalarExpression).Expression;
                        if (expression is Literal)
                        {

                        }
                        if (expression is ColumnReferenceExpression)
                        {

                        }
                        if (expression is VariableReference)
                        {

                        }
                        if (expression is ScalarSubquery)
                        {
                            GetScalarSubquery(expression as ScalarSubquery);
                        }
                    }
                    if (element is SelectStarExpression)
                    {
                        AddMessage(Code.T0000008, element, target);
                    }
                }
            }
        }
        public void CheckStatment(SetVariableStatement set)
        {
            var setResult = new SetVariableStatement();

            if (set.Expression is BinaryExpression)
            {
                set.Expression = calculateExpression(set.Expression as BinaryExpression);
            }
            setResult.Expression = convertExpression(set.Expression);

            DeclareVariableElement declareVariableElement = this.GetVariableReference(set.Variable);
            declareVariableElement.Value = setResult.Expression;

            DeclareVariableElementChecked(declareVariableElement);
        }
        public void CheckStatment(DeclareVariableStatement dec)
        {
            foreach (DeclareVariableElement declar in dec.Declarations)
            {
                if (varible.ContainsKey(declar.VariableName.Value))
                {
                    AddMessage(Code.T0000003, declar, declar.VariableName.Value);
                    continue;
                }
                varible.Add(declar.VariableName.Value, new ReferCount<DeclareVariableElement, int>(declar.CloneObject(), 0));
                DeclareVariableElementChecked(declar);
            }
        }
        public void CheckStatment(SelectStatement select)
        {
            if (select.WithCtesAndXmlNamespaces != null && select.WithCtesAndXmlNamespaces is WithCtesAndXmlNamespaces)
            {
                foreach (var item in select.WithCtesAndXmlNamespaces.CommonTableExpressions)
                {
                    getQuerySpecification(item.QueryExpression as QuerySpecification, false);
                    addWithTable(item);
                }
            }
            if (select.QueryExpression != null && select.QueryExpression is QuerySpecification)
            {
                getQuerySpecification(select.QueryExpression as QuerySpecification);

            }
        }
        private void CheckStatment(IfStatement ifStatement)
        {
            if (ifStatement.Predicate is BooleanExpression)
            {
                this.checkedBooleanComparison(ifStatement.Predicate as BooleanExpression);
            }

            CheckStatments(new[] { ifStatement.ThenStatement });
            CheckStatments(new[] { ifStatement.ElseStatement });
        }
        private void CheckStatment(AlterProcedureStatement c)
        {
            foreach (var param in c.Parameters)
            {
                parametrs.Add(param.VariableName.Value, new ReferCount<ProcedureParameter, int>(param, 0));
            }
            CheckStatments((c).StatementList.Statements);
            this.PostAllStatmentChecable();
            this.clearObjectFromStatement();
        }
        private void CheckStatment(CreateProcedureStatement c)
        {
            foreach (var param in c.Parameters)
            {
                parametrs.Add(param.VariableName.Value, new ReferCount<ProcedureParameter, int>(param, 0));
            }
            CheckStatments(c.StatementList.Statements);
            this.PostAllStatmentChecable();
            this.clearObjectFromStatement();
        }
        internal void CheckStatment(DropTableStatement dropTableStatement)
        {
            foreach (var item in dropTableStatement.Objects)
            {
                if (item is SchemaObjectName)
                {
                    string table = getNameIdentifiers((item as SchemaObjectName));
                    var compare = compareNull.SingleOrDefault(c => string.Compare(c.Key, table) == 0);
                    if (compare.Key == null)
                    {
                        AddMessage(Code.T0000044, dropTableStatement, table);
                    }

                    dropTebles.Add(table, new ReferCount<SchemaObjectName, int>(item, 0));
                }
            }
        }
        #endregion

        public void checkedBooleanComparison(BooleanExpression booleanExpression)
        {
            if (booleanExpression is BooleanBinaryExpression)
            {
                checkedBooleanComparison((booleanExpression as BooleanBinaryExpression).FirstExpression);
                checkedBooleanComparison((booleanExpression as BooleanBinaryExpression).SecondExpression);
            }
            if (booleanExpression is BooleanComparisonExpression)
            {
                checkedBooleanComparisonExpression(booleanExpression as BooleanComparisonExpression);
            }
            if (booleanExpression is BooleanIsNullExpression)
            {
                checkedBooleanIsNullExpression(booleanExpression as BooleanIsNullExpression);
            }
            if (booleanExpression is ExistsPredicate)
            {

            }
        }

        internal void clearObjectFromStatement()
        {
            IsAliasAll = true;
            tables.Clear();
            withTables.Clear();
            parametrs.Clear();
        }
        internal void clearObjectFromFile()
        {
            tempTeble.Clear();
        }
        internal void clearObjectFromBatche()
        {
            varible.Clear();
            tableVarible.Clear();
        }
        private void DeclareVariableElementChecked(DeclareVariableElement var)
        {
            switch (var.Value)
            {
                case Literal literal:
                    {
                        CheckVariableLiteral(var, literal);
                        break;
                    }
                case ScalarSubquery s:
                    Literal literal = GetScalarSubquery(s);
                    CheckVariableLiteral(var, literal);
                    break;
                case CastCall s:
                    Literal literal1 = getConvertOrCast(s);
                    CheckVariableLiteral(var, literal1);
                    break;
                default:
                    throw new ExceptionTSqlFragment(var.Value);
            }
        }

        private void CheckVariableLiteral(DeclareVariableElement var, Literal literal)
        {
            var DataType = var.DataType as SqlDataTypeReference;

            bool isCheck = CheckTypeDeclareAndValue(DataType, literal.LiteralType);
            if (!isCheck) return;
            if ((DataType.SqlDataTypeOption == SqlDataTypeOption.NVarChar
                || DataType.SqlDataTypeOption == SqlDataTypeOption.VarChar)
                && literal != null
                && literal.Value != null
                )
            {
                if (string.Compare(DataType.Parameters[0].Value, "max", true) == 0)
                {
                    DataType.Parameters[0].Value = "8000";
                }
                int len = int.Parse(DataType.Parameters[0].Value);
                if (len < literal.Value.Length)
                {
                    string varName = var.VariableName.Value;
                    string lenVar = len.ToString();
                    string lenVul = literal.Value.Length.ToString();
                    this.AddMessage(Code.T0000002, var, varName, lenVul, lenVar);
                }
            }
        }

        private bool CheckTypeDeclareAndValue(SqlDataTypeReference dataType, LiteralType literalType)
        {
            if (literalType == LiteralType.String
                && (dataType.SqlDataTypeOption == SqlDataTypeOption.NVarChar
                || dataType.SqlDataTypeOption == SqlDataTypeOption.VarChar)
                )
            {
                return true;
            }
            if (literalType == LiteralType.Integer
                && (dataType.SqlDataTypeOption == SqlDataTypeOption.Int
                || dataType.SqlDataTypeOption == SqlDataTypeOption.Decimal
                || dataType.SqlDataTypeOption == SqlDataTypeOption.Bit
                || dataType.SqlDataTypeOption == SqlDataTypeOption.Float)
                )
            {
                return true;
            }

            this.AddMessage(Code.T0000047, dataType, dataType.SqlDataTypeOption.ToString(), literalType.ToString()
                );
            return false;
        }
        internal void PostBatcheChecable()
        {
            ///проверка закрытия транзакций
            ///проверка использвание переменных
            ///проверка использвание аргументов
            CheckUsingVariable();
            CheckUsengTableVarible();
        }
        private Literal GetLiteral(VariableReference variableReference)
        {
            var res = GetVariableReference(variableReference).Value;
            switch (res)
            {
                case Literal r: return r;
                case BinaryExpression r: return calculateExpression(r);
                default:
                    break;
            }
            return null;
        }
        private DeclareVariableElement GetVariableReference(VariableReference variableReference)
        {
            if (string.IsNullOrEmpty(variableReference.Name))
            {
                throw new Exception(string.Format("Переменная не определена"));
            }
            if (varible.ContainsKey(variableReference.Name))
            {
                varible[variableReference.Name].Count++;
                return varible[variableReference.Name].Obj;
            }
            else
            if (parametrs.ContainsKey(variableReference.Name))
            {
                parametrs[variableReference.Name].Count++;
                return parametrs[variableReference.Name].Obj;
            }
            AddMessage(Code.T0000004, variableReference, variableReference.Name);
            return null;
        }
        CreateTableStatement getTemptable(string name)
        {
            if (!tempTeble.ContainsKey(name))
            {
                AddMessage(Code.T0000016, null, name);
                return null;
            }

            tempTeble[name].Count++;
            return tempTeble[name].Obj;
        }
        bool IsTempTable(string name)
        {
            return name.Length > 0 && name[0] == '#'
                || (name.Length > 1 && name[0] == '#' && name[1] == '#');
        }


        #region StatementsFunction
        private void addWithTable(CommonTableExpression with)
        {
            string key = with.ExpressionName.Value;
            if (withTables.ContainsKey(key))
            {
                AddMessage(Code.T0000018, with, key);
                return;
            }

            withTables.Add(key, new ReferCount<CommonTableExpression, int>(with, 0));
        }
        private void getQuerySpecification(QuerySpecification Query, bool IsAddTableList = true)
        {
            if (Query.FromClause != null)
            {
                var from = Query.FromClause as FromClause;
                foreach (TableReference tableReference in from.TableReferences)
                {
                    CheckeTableReference(tableReference);
                }
            }
            foreach (var element in Query.SelectElements)
            {
                if (element is SelectScalarExpression)
                {
                    var expression = (element as SelectScalarExpression).Expression;
                    switch (expression)
                    {
                        case VariableReference ex: GetVariableReference(ex); break;
                        case ColumnReferenceExpression ex: checkedColumnReference(ex); break;
                        case StringLiteral ex:
                            break;
                        default:
                            throw new ExceptionTSqlFragment(expression);
                    }
                }
            }
            if (Query.WhereClause != null)
            {
                checkedBooleanComparison(Query.WhereClause.SearchCondition);
            }
        }
        private void CheckeTableReference(TableReference tableReference)
        {
            if (tableReference is QualifiedJoin)
            {
                var join = tableReference as QualifiedJoin;
                CheckeTableReference(join.FirstTableReference);
                CheckeTableReference(join.SecondTableReference);
                //AddTable(first);
                //AddTable(second);
                checkedBooleanComparison(join.SearchCondition);
            }
            if (tableReference is NamedTableReference)
            {
                if (IsTempTable((tableReference as NamedTableReference).SchemaObject.BaseIdentifier.Value))
                {
                    getTemptable((tableReference as NamedTableReference).SchemaObject.BaseIdentifier.Value);
                }
                else
                {
                    if (server.ConnectionContext.IsOpen)
                        GetObjectFromServer(tableReference);
                }
                AddTable(tableReference as NamedTableReference);
            }
            if (tableReference is VariableTableReference)
            {
                getDeclareTableVariable(tableReference as VariableTableReference);
            }
            if (tableReference is QueryDerivedTable)
            {
                var query = tableReference as QueryDerivedTable;
                getQuerySpecification(query.QueryExpression as QuerySpecification);
            }
        }
        //private void checkedSearchCondition(BooleanExpression searchCondition)
        //{
        //    if (searchCondition is BooleanBinaryExpression)
        //    {
        //        var search = searchCondition as BooleanBinaryExpression;
        //        checkedBooleanComparison(search);
        //    }
        //    if (searchCondition is BooleanComparisonExpression)
        //    {
        //        var search = searchCondition as BooleanComparisonExpression;
        //        checkedBooleanComparison(search);
        //    }
        //}
        #endregion

        private Literal calculateExpression(BinaryExpression expr)
        {
            if (expr.FirstExpression is BinaryExpression)
            {
                expr.FirstExpression = calculateExpression(expr.FirstExpression as BinaryExpression);
            }

            expr.FirstExpression = convertExpression(expr.FirstExpression);
            expr.SecondExpression = convertExpression(expr.SecondExpression);

            if (expr.FirstExpression is Literal && expr.SecondExpression is Literal)
            {
                if (expr.BinaryExpressionType == BinaryExpressionType.Add)
                {
                    if (expr.FirstExpression is StringLiteral && expr.SecondExpression is StringLiteral)
                        return Calculate<StringLiteral, string>(expr, (a) => a, (a, b) => { return a + b; });
                    if (expr.FirstExpression is IntegerLiteral && expr.SecondExpression is IntegerLiteral)
                        return Calculate<IntegerLiteral, int>(expr, int.Parse, (a, b) => { return a + b; });
                    if (expr.FirstExpression is NumericLiteral && expr.SecondExpression is NumericLiteral)
                        return Calculate<IntegerLiteral, float>(expr, float.Parse, (a, b) => { return a + b; });
                }
                else
                {
                    if (expr.FirstExpression is StringLiteral && expr.SecondExpression is StringLiteral)
                        AddMessage(Code.T0000005, expr.FirstExpression, expr.BinaryExpressionType.ToString());
                }
                if (expr.BinaryExpressionType == BinaryExpressionType.Subtract)
                {
                    if (expr.FirstExpression is IntegerLiteral && expr.SecondExpression is IntegerLiteral)
                        return Calculate<IntegerLiteral, int>(expr, int.Parse, (a, b) => { return a - b; });
                    if (expr.FirstExpression is NumericLiteral && expr.SecondExpression is NumericLiteral)
                        return Calculate<IntegerLiteral, float>(expr, float.Parse, (a, b) => { return a - b; });
                }
                if (expr.BinaryExpressionType == BinaryExpressionType.Multiply)
                {
                    if (expr.FirstExpression is IntegerLiteral && expr.SecondExpression is IntegerLiteral)
                        return Calculate<IntegerLiteral, int>(expr, int.Parse, (a, b) => { return a * b; });
                    if (expr.FirstExpression is NumericLiteral && expr.SecondExpression is NumericLiteral)
                        return Calculate<IntegerLiteral, float>(expr, float.Parse, (a, b) => { return a * b; });
                }
                if (expr.BinaryExpressionType == BinaryExpressionType.Divide)
                {
                    if (expr.FirstExpression is IntegerLiteral && expr.SecondExpression is IntegerLiteral)
                        return Calculate<IntegerLiteral, int>(expr, int.Parse, (a, b) => { return a / b; });
                    if (expr.FirstExpression is NumericLiteral && expr.SecondExpression is NumericLiteral)
                        return Calculate<IntegerLiteral, float>(expr, float.Parse, (a, b) => { return a / b; });
                }
            }
            return null;
        }
        T Calculate<T, T1>(BinaryExpression val, Func<string, T1> parse, Func<T1, T1, T1> result)
             where T : Literal, new()
        {
            T res = new T();
            Literal literaFirst = val.FirstExpression as Literal;
            Literal literalSecond = val.SecondExpression as Literal;
            res.Value = result(parse(literaFirst.Value), parse(literalSecond.Value)).ToString();
            return res;
        }
        DeclareTableVariableBody getDeclareTableVariable(VariableTableReference vtr)
        {
            if (!tableVarible.ContainsKey(vtr.Variable.Name))
            {
                AddMessage(Code.T0000015, vtr, vtr.Variable.Name);
                return null;
            }
            tableVarible[vtr.Variable.Name].Count++;
            return tableVarible[vtr.Variable.Name].Obj;
        }
        private ScalarExpression convertExpression(ScalarExpression expression)
        {
            if (expression is Literal)
            {
                return expression;
            }
            if (expression is ConvertCall || expression is CastCall)
            {
                return getConvertOrCast(expression);
            }
            if (expression is VariableReference)
            {
                return GetLiteral(expression as VariableReference);
            }
            if (expression is FunctionCall)
            {
                return getResultFunctionCall(expression as FunctionCall);
            }
            if (expression is LeftFunctionCall)
            {
                return new StringLiteral();
            }
            if (expression is RightFunctionCall)
            {
                return new StringLiteral();
            }
            return new StringLiteral();
        }
        private Literal GetScalarSubquery(ScalarSubquery subquery)
        {
            Literal resultLiteral = null;
            switch (subquery.QueryExpression)
            {
                case QuerySpecification queryPart:
                    {
                        if (queryPart.TopRowFilter != null)
                        {
                            var p = queryPart.TopRowFilter.Expression as ParenthesisExpression;
                            //Literal literal = null;
                            if (p.Expression is VariableReference)
                            {
                                resultLiteral = GetLiteral(p.Expression as VariableReference);
                            }
                            else
                            if (p.Expression is IntegerLiteral)
                            {
                                resultLiteral = p.Expression as IntegerLiteral;
                            }
                            if (resultLiteral == null)
                            {
                                AddMessage(Code.T0000036, subquery, p.Expression.GetType().Name);
                            }
                            else
                            {
                                if (int.Parse(resultLiteral.Value) != 1)
                                {
                                    AddMessage(Code.T0000035, subquery, resultLiteral.Value);
                                }
                            }
                        }
                        else
                        {
                            if (queryPart.FromClause != null)
                                AddMessage(Code.T0000034, subquery);
                        }

                        if (queryPart.WhereClause == null && queryPart.FromClause != null)
                        {
                            AddMessage(Code.T0000037, subquery);
                        }

                        //getQuerySpecification(queryPart);

                        if (queryPart.SelectElements.Count > 1)
                        {
                            AddMessage(Code.T0000012, subquery);
                        }
                        else
                        if (queryPart.SelectElements.Count == 1)
                        {
                            var el = queryPart.SelectElements[0];
                            switch (el)
                            {
                                case SelectStarExpression el1: AddMessage(Code.T0000013, subquery); break;
                                case SelectScalarExpression expression1:
                                    var expression = (el as SelectScalarExpression).Expression;
                                    switch (expression)
                                    {
                                        case Literal ex: resultLiteral = ex; break;
                                        case ColumnReferenceExpression ex: break;
                                        case VariableReference ex: break;
                                        case ScalarSubquery ex: GetScalarSubquery(ex); break;
                                        default: throw new ExceptionTSqlFragment(expression);
                                    }
                                    break;
                                default:
                                    throw new ExceptionTSqlFragment(el);
                            }
                        }
                        break;
                    }
                default:
                    throw new ExceptionTSqlFragment(subquery.QueryExpression);
            }
            return resultLiteral;
        }
        private Literal getConvertOrCast(ScalarExpression secondExpression)
        {
            dynamic castCall = secondExpression as CastCall;
            if (secondExpression is ConvertCall)
                castCall = secondExpression as ConvertCall;

            if (castCall.Parameter is VariableReference)
            {
                castCall.Parameter = GetLiteral(castCall.Parameter as VariableReference);
            }

            StringLiteral sl = new StringLiteral();
            if (castCall.DataType is SqlDataTypeReference)
            {
                var DataType = castCall.DataType as SqlDataTypeReference;
                if (DataType.SqlDataTypeOption == SqlDataTypeOption.NVarChar || DataType.SqlDataTypeOption == SqlDataTypeOption.VarChar)
                {
                    if (castCall.Parameter is IntegerLiteral)
                    {
                        sl.Value = (castCall.Parameter as Literal).Value;
                    }
                }
            }
            return sl;
        }
        private Literal getResultFunctionCall(FunctionCall functionCall)
        {
            Console.WriteLine("Нужно дописать выполнение скалярной функции!");

            return new StringLiteral();
        }
        void checkedBooleanComparisonExpression(BooleanComparisonExpression search)
        {
            if (search.FirstExpression is ColumnReferenceExpression
                && search.SecondExpression is ColumnReferenceExpression)
            {
                MyColumn firstColumn = checkedColumnReference(search.FirstExpression as ColumnReferenceExpression);
                MyColumn secondColumn = checkedColumnReference(search.SecondExpression as ColumnReferenceExpression);

                if (firstColumn.IsValid && firstColumn.IsValid && firstColumn != null && secondColumn != null)
                {
                    if (server.ConnectionContext.IsOpen)
                        checkColumnFromServer(firstColumn.Column, secondColumn.Column);
                }

                if (((firstColumn.Alias == null && secondColumn.Alias != null)
                     || (firstColumn.Alias != null && secondColumn.Alias == null))
                    && string.Compare(firstColumn.Name, secondColumn.Name, true) == 0)
                {
                    AddMessage(Code.T0000031, search, secondColumn.Name);
                }

                if (firstColumn.Alias != null && secondColumn.Alias != null)
                {
                    if (string.Compare(firstColumn.Alias, secondColumn.Alias, true) == 0
                    && string.Compare(firstColumn.Name, secondColumn.Name, true) == 0
                    )
                    {
                        AddMessage(Code.T0000032, search, firstColumn.Alias);
                    }
                    else
                    if (firstColumn.Alias != null && secondColumn.Alias != null
                        && string.Compare(firstColumn.Alias, secondColumn.Alias, true) == 0)
                    {
                        AddMessage(Code.T0000025, search, firstColumn.Alias);
                    }
                }
            }
            else
            {
                if (search.FirstExpression is ColumnReferenceExpression)
                {
                    MyColumn firstColumn = checkedColumnReference(search.FirstExpression as ColumnReferenceExpression);

                    if (search.SecondExpression is VariableReference)
                    {

                    }
                    if (search.SecondExpression is Literal)
                    {
                        if (search.SecondExpression is NullLiteral)
                        {
                            if (search.ComparisonType == BooleanComparisonType.Equals)
                            {
                                AddMessage(Code.T0000020, search, firstColumn.FullName, search.ComparisonType.ToString());
                            }
                        }
                    }
                }
                else if (search.SecondExpression is ColumnReferenceExpression)
                {
                    MyColumn secondColumn = checkedColumnReference(search.SecondExpression as ColumnReferenceExpression);

                    if (search.FirstExpression is VariableReference)
                    {

                    }
                    if (search.FirstExpression is Literal)
                    {

                    }
                }
                else
                {

                }
            }
        }
        string getBaseIdentifier(string str)
        {
            string res = "";
            if (!str.Contains('.'))
            {
                res = str.Replace("[", "").Replace("]", "");
            }
            else
            {
                var r = str.Split(new[] { "].[", ".[", "].", "." }, StringSplitOptions.None);
                if (r.Length > 0)
                {
                    res = r[r.Length - 1].Replace("[", "").Replace("]", "");
                }
            }
            return res;
        }
        private void checkedBooleanIsNullExpression(BooleanIsNullExpression booleanIsNullExpression)
        {
            if (booleanIsNullExpression.Expression is ColumnReferenceExpression)
            {
                var ex = booleanIsNullExpression.Expression as ColumnReferenceExpression;
                //проверить что поле может принимать значение null, иначе сравнение не корректно
                //addMessage(Code.T0000021, getNameColumn(ex.MultiPartIdentifier.Identifiers));
            }

            if (booleanIsNullExpression.Expression is FunctionCall)
            {
                var func = booleanIsNullExpression.Expression as FunctionCall;
                if (string.Compare(func.FunctionName.Value, "object_id", true) == 0)
                {
                    if (func.Parameters.Count > 0)
                    {
                        bool isValid = true;
                        foreach (var item in func.Parameters)
                        {
                            if (!(item is StringLiteral))
                            {
                                isValid = false;
                                AddMessage(Code.T0000042, booleanIsNullExpression);
                                break;
                            }
                        }
                        if (isValid)
                        {
                            var par0 = (func.Parameters[0] as StringLiteral).Value;
                            string baseIdentifier = getBaseIdentifier(par0);
                            if (string.IsNullOrEmpty(baseIdentifier))
                            {
                                AddMessage(Code.T0000043, booleanIsNullExpression, par0);
                                return;
                            }
                            compareNull.Add(baseIdentifier, new ReferCount<BooleanIsNullExpression, int>(booleanIsNullExpression, 0));
                        }
                    }
                }
            }
        }
        private MyColumn checkedColumnReference(ColumnReferenceExpression Expression)
        {
            var column = new MyColumn(null);
            var Identifiers = Expression.MultiPartIdentifier.Identifiers;
            column.Alias = Identifiers.Count > 1 ? Identifiers[Identifiers.Count - 2].Value : null;
            column.Name = Identifiers[Identifiers.Count - 1].Value;

            column.IsValid = !(Identifiers.Count > 2);

            if (!column.IsValid)
            {
                AddMessage(Code.T0000027, Expression, getNameIdentifiers(Expression.MultiPartIdentifier));
            }
            if (IsAliasAll && column.Alias == null)
            {
                AddMessage(Code.T0000019, Expression, column.Name);
            }
            if (column.IsValid && server.ConnectionContext.IsOpen)
            {
                column.Column = checableColumnFromServer(column.Alias, column.Name)?.Column;
            }
            return column;
        }
        private void checkColumnFromServer(Column firstColumn, Column secondColumn)
        {
#warning включить проверки по типам
            if (firstColumn != null && secondColumn != null)
            {
                if (firstColumn.DataType != secondColumn.DataType)
                {
                    AddMessage(Code.T0000046, null);
                    //addMessage(new MyTyps("типы для таблиц не равны!",TypeMessage.Error),null);
                }
            }
        }
        private MyColumn checableColumnFromServer(string firstAlias, string firstname)
        {
            MyColumn column = null;
            if (firstAlias != null)
            {
                var t = getTableFromAlias(firstAlias);
                if (t is NamedTableReference)
                {
                    var table = GetObjectFromServer(t as NamedTableReference) as Table;

                    //if (!table.IsExists)
                    //{
                    //    addMessage(Code.T0000028, table.Name);
                    //}

                    column = table.Columns.SingleOrDefault(c => string.Compare(c.Name, firstname, true) == 0);
                    if (column == null)
                    {
                        AddMessage(Code.T0000029, t, firstname);
                    }
                }
            }
            else
            {
                List<MyColumn> cols = new List<MyColumn>();
                foreach (var table in tables)
                {
                    var tableFromServer = GetObjectFromServer(table.Value.Obj as NamedTableReference) as Table;
                    if (tableFromServer.IsExists)
                    {
                        cols.AddRange(tableFromServer.Columns.Where(c => string.Compare(c.Name, firstname, true) == 0).ToList());

                        if (cols.Count > 1)
                            break;
                    }
                }
                if (cols.Count == 1)
                {
                    return cols[0];
                }
                if (cols.Count > 1)
                {
                    AddMessage(Code.T0000033, null, firstname);
                }
                AddMessage(Code.T0000030, null, firstname);
            }
            return column;
        }
        private TSqlFragment getTableFromAlias(string alias)
        {
            bool T = tables.ContainsKey(alias);
            bool W = withTables.ContainsKey(alias);
            if (!T && !W)
            {
                AddMessage(Code.T0000014, null, alias);
            }
            else
            if (T)
            {
                tables[alias].Count++;
                return tables[alias].Obj;
            }
            else
            if (W)
            {
                withTables[alias].Count++;
                return withTables[alias].Obj;
            }
            return null;
        }
        private void getTableServerFromAlias(string firstAlias)
        {
            throw new NotImplementedException();
        }
        private string getNameIdentifiers(MultiPartIdentifier multiPart)
        {
            return string.Join(".", multiPart.Identifiers.Select(c => c.Value));
        }
        private string getNameTable(TableReference table)
        {
            if (table is NamedTableReference)
            {
                var named = table as NamedTableReference;
                return getNameIdentifiers(named.SchemaObject);
            }
            return null;
        }
        private string getAliasTable(TableReference table)
        {
            if (table is NamedTableReference)
            {
                return (table as NamedTableReference)?.Alias?.Value;
            }
            if (table is VariableTableReference)
            {
                return (table as VariableTableReference)?.Alias?.Value;
            }
            if (table is QueryDerivedTable)
            {
                return (table as QueryDerivedTable)?.Alias?.Value;
            }
            return null;
        }
        void AddTable(TableReference tableReference)
        {
            string key = getAliasTable(tableReference);
            if (key == null)
            {
                IsAliasAll = false;
                key = getNameTable(tableReference);
            }
            if (tables.ContainsKey(key))
            {
                AddMessage(Code.T0000017, tableReference, key, getNameTable(tables[key].Obj));
                return;
            }
            tables.Add(key, new ReferCount<TableReference, int>(tableReference, 0));
        }


        public void CheckStatments(IList<TSqlStatement> statements)
        {
            foreach (TSqlStatement statement in statements)
            {
                if (statement == null) continue;

                switch (statement)
                {
                    case CreateTableStatement c: this.CheckStatment(c); break;
                    case WhileStatement c: CheckStatments(new[] { c.Statement }); break;
                    case BeginEndBlockStatement c: CheckStatments(c.StatementList.Statements); break;
                    case CreateProcedureStatement c: CheckStatment(c); break;
                    case AlterProcedureStatement c: CheckStatment(c); break;
                    case ProcedureStatementBodyBase c: CheckStatments(c.StatementList.Statements); break;
                    case IfStatement c: CheckStatment(c); break;
                    case SetVariableStatement c: this.CheckStatment(c); break;
                    case DeclareVariableStatement c: this.CheckStatment(c); break;
                    case DeclareTableVariableStatement c: this.CheckStatment(c); break;
                    case SelectStatement c: this.CheckStatment(c); break;
                    case InsertStatement c: this.CheckStatment(c); break;
                    case DropTableStatement c: this.CheckStatment(c); break;
                    default: throw new ExceptionTSqlFragment(statement);
                }
            }
        }
    }
}
