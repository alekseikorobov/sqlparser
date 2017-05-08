using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using sqlparser.Modele;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sqlparser
{
    public class ObjectFromServer
    {

    }
    public class Table : ObjectFromServer
    {
        public Table()
        {
            IsExists = false;
            Columns = new List<MyColumn>();
        }
        public Table(string name) : this()
        {
            Name = name;
        }
        public bool IsExists { get; set; }
        string name;
        public string Name { get { return name; } set { name = value; IsExists = true; } }
        public List<MyColumn> Columns { get; set; }
    }
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
    public class Message
    {
        public Message()
        {
            Messages = new List<Message>();
        }
        public void addMessage(Code code, params string[] data)
        {
            Messages.Add(new Message(code, data));
        }
        public void addMessage(String text, params string[] data)
        {
            Messages.Add(new Message(text, data));
        }
        public List<Message> Messages { get; set; }
        private string text;

        public Message(Code code, string[] data)
        {
            this.Code = code;
            this.Data = data;
        }
        public Message(String text, string[] data)
        {
            this.text = text;
            this.Data = data;
        }
        public string MessageInformation
        {
            get
            {
                return string.Format("(" + Code.Value.ToString() + ") " + Text, Data);
            }
        }
        public Code? Code { get; set; }
        public string[] Data { get; set; }
        public string Text
        {
            get
            {
                return Code.HasValue ? DictionaryMessage.GetMessage(Code.Value) : text;
            }
            private set
            {
                text = value;
            }
        }
    }
    public class ReferCount<T1, T2>
    {
        public ReferCount(T1 obj, T2 count)
        {
            Obj = obj;
            Count = count;
        }

        public T1 Obj { get; }
        public T2 Count { get; set; }
    }
    public class Chekable
    {
        Dictionary<string, Table> TableFromServer = new Dictionary<string, Table>();
        string database = "";
        string schema = "dbo";
        string serverName = "RUMSKR90AZ5WD";
        bool IsAliasAll = true;
        public Message messages = new Message();
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
                        messages.addMessage(Code.T0000022, fullNameTable, obj.GetType().Name);
                    }
                }
                else
                {
                    messages.addMessage(Code.T0000023, fullNameTable);
                    TableFromServer.Add(fullNameTable, temptable);
                }
                return temptable;
            }
            return null;
        }
        Server server;
        public Chekable()
        {
            server = new Server();
            server.ConnectionContext.ConnectionString = "data source=(local);initial catalog=master;integrated security=True;application name=master;MultipleActiveResultSets=True";
            server.ConnectionContext.Connect();
            var conect = (server.ConnectionContext.SqlConnectionObject as SqlConnection);
            serverName = conect.WorkstationId;
            database = conect.Database;
            schema = server.Databases[database].DefaultSchema;
        }

        Dictionary<string, ReferCount<DeclareVariableElement, int>> varible
            = new Dictionary<string, ReferCount<DeclareVariableElement, int>>();

        Dictionary<string, ReferCount<CreateTableStatement, int>> tempTeble
            = new Dictionary<string, ReferCount<CreateTableStatement, int>>();

        Dictionary<string, ReferCount<DeclareTableVariableBody, int>> tableVarible
            = new Dictionary<string, ReferCount<DeclareTableVariableBody, int>>();

        Dictionary<string, ReferCount<TableReference, int>> tables
            = new Dictionary<string, ReferCount<TableReference, int>>();

        Dictionary<string, ReferCount<CommonTableExpression, int>> withTables
            = new Dictionary<string, ReferCount<CommonTableExpression, int>>();

        internal void clearObjectFromStatement()
        {
            IsAliasAll = true;
            tables.Clear();
            withTables.Clear();
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
        public void CheckUsengTableVarible()
        {
            foreach (var table in tableVarible)
            {
                if (table.Value.Count == 0)
                {
                    messages.addMessage(Code.T0000011, table.Key);
                }
            }
        }
        public void CheckUsingVariable()
        {
            foreach (var value in varible)
            {
                if (value.Value.Count == 0)
                {
                    messages.addMessage(Code.T0000001, value.Value.Obj.VariableName.Value);
                }
            }
        }
        private void allChecked(DeclareVariableElement var)
        {
            if (var.Value is StringLiteral && var.DataType is SqlDataTypeReference)
            {
                StringLiteral stringLiteral = var.Value as StringLiteral;
                var DataType = var.DataType as SqlDataTypeReference;

                if ((DataType.SqlDataTypeOption == SqlDataTypeOption.NVarChar
                        || DataType.SqlDataTypeOption == SqlDataTypeOption.VarChar)
                    && stringLiteral != null && stringLiteral.Value != null
                    )
                {
                    if (string.Compare(DataType.Parameters[0].Value, "max", true) == 0)
                    {
                        DataType.Parameters[0].Value = "8000";
                    }
                    int len = int.Parse(DataType.Parameters[0].Value);
                    if (len < stringLiteral.Value.Length)
                    {
                        messages.addMessage(Code.T0000002, var.VariableName.Value);
                    }
                }
            }
            if (var.Value is ScalarSubquery)
            {
                getScalarSubquery(var.Value as ScalarSubquery);
            }
        }
        public void PostFileChecable()
        {
            ///проверка удаления временных таблиц            
        }
        internal void PostBatcheChecable()
        {
            ///проверка закрытия транзакций
            ///проверка использвание переменных
            ///проверка использвание аргументов
            CheckUsingVariable();
            CheckUsengTableVarible();
        }
        private Literal getLiteral(VariableReference variableReference)
        {
            var res = getDeclare(variableReference).Value;
            if (res is Literal)
            {
                return res as Literal;
            }
            else
            if (res is BinaryExpression)
            {
                res = calculateExpression(res as BinaryExpression);
                //getBooleanBinaryExpression(res as BinaryExpression);
            }
            return res as Literal;
        }
        private DeclareVariableElement getDeclare(VariableReference variableReference)
        {
            if (string.IsNullOrEmpty(variableReference.Name))
            {
                throw new Exception(string.Format("Переменная не определена"));
            }
            if (!varible.ContainsKey(variableReference.Name))
            {
                messages.addMessage(Code.T0000004, variableReference.Name);
                return null;
            }
            varible[variableReference.Name].Count++;
            return varible[variableReference.Name].Obj;
        }
        CreateTableStatement getTemptable(string name)
        {
            if (!tempTeble.ContainsKey(name))
            {
                messages.addMessage(Code.T0000016, name);
                return null;
            }

            tempTeble[name].Count++;
            return tempTeble[name].Obj;
        }

        #region Statements

        bool IsTempTable(string name)
        {
            return name.Length > 0 && name[0] == '#'
                || (name.Length > 1 && name[0] == '#' && name[1] == '#');
        }
        public void getCreateTableStatement(CreateTableStatement createTableStatement)
        {
            var ident = createTableStatement.SchemaObjectName.BaseIdentifier.Value;
            if (IsTempTable(ident))
            {
                tempTeble.Add(ident, new ReferCount<CreateTableStatement, int>(createTableStatement, 0));
            }
            //SchemaObjectName
        }
        public void getDeclareTableVariableStatement(DeclareTableVariableStatement declareTableVariableStatement)
        {
            tableVarible.Add(declareTableVariableStatement.Body.VariableName.Value, new ReferCount<DeclareTableVariableBody, int>(declareTableVariableStatement.Body, 0));
        }
        public void getInsertStatement(InsertStatement statement)
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
                    table = GetObjectFromServer(Target) as Table;
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
                messages.addMessage(Code.T0000007, target);
            }
            if (statement.InsertSpecification.InsertSource is SelectInsertSource)
            {
                var select = statement.InsertSpecification.InsertSource as SelectInsertSource;
                var query = select.Select as QuerySpecification;
                if (query.SelectElements.Count > statement.InsertSpecification.Columns.Count)
                {
                    messages.addMessage(Code.T0000009, target);
                }
                else
                if (query.SelectElements.Count < statement.InsertSpecification.Columns.Count)
                {
                    messages.addMessage(Code.T0000010, target);
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
                            getScalarSubquery(expression as ScalarSubquery);
                        }
                    }
                    if (element is SelectStarExpression)
                    {
                        messages.addMessage(Code.T0000008, target);
                    }
                }
            }
        }


        public void getSetVariableStatement(SetVariableStatement set)
        {
            var setResult = new SetVariableStatement();

            var var = getDeclare(set.Variable);

            if (set.Expression is BinaryExpression)
            {
                set.Expression = calculateExpression(set.Expression as BinaryExpression);
            }
            setResult.Expression = convertExpression(set.Expression);

            var.Value = setResult.Expression;

            allChecked(var);
        }
        public void getDeclareVariableStatement(DeclareVariableStatement dec)
        {
            foreach (var declar in dec.Declarations)
            {
                if (varible.ContainsKey(declar.VariableName.Value))
                {
                    messages.addMessage(Code.T0000003, declar.VariableName.Value);
                    continue;
                }
                varible.Add(declar.VariableName.Value, new ReferCount<DeclareVariableElement, int>(CloneObject(declar) as DeclareVariableElement, 0));
                allChecked(declar);
            }
        }
        public void getSelectStatement(SelectStatement select)
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
        private void addWithTable(CommonTableExpression with)
        {
            string key = with.ExpressionName.Value;
            if (withTables.ContainsKey(key))
            {
                messages.addMessage(Code.T0000018, key);
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
                    var expression = element as SelectScalarExpression;
                    if (expression.Expression is VariableReference)
                    {
                        getDeclare(expression.Expression as VariableReference);
                    }
                    if (expression.Expression is ColumnReferenceExpression)
                    {
                        var myColumn = checkedColumnReference(expression.Expression as ColumnReferenceExpression);
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
                        return calculate<StringLiteral, string>(expr, (a) => a, (a, b) => { return a + b; });
                    if (expr.FirstExpression is IntegerLiteral && expr.SecondExpression is IntegerLiteral)
                        return calculate<IntegerLiteral, int>(expr, int.Parse, (a, b) => { return a + b; });
                    if (expr.FirstExpression is NumericLiteral && expr.SecondExpression is NumericLiteral)
                        return calculate<IntegerLiteral, float>(expr, float.Parse, (a, b) => { return a + b; });
                }
                else
                {
                    if (expr.FirstExpression is StringLiteral && expr.SecondExpression is StringLiteral)
                        messages.addMessage(Code.T0000005, expr.BinaryExpressionType.ToString());
                }
                if (expr.BinaryExpressionType == BinaryExpressionType.Subtract)
                {
                    if (expr.FirstExpression is IntegerLiteral && expr.SecondExpression is IntegerLiteral)
                        return calculate<IntegerLiteral, int>(expr, int.Parse, (a, b) => { return a - b; });
                    if (expr.FirstExpression is NumericLiteral && expr.SecondExpression is NumericLiteral)
                        return calculate<IntegerLiteral, float>(expr, float.Parse, (a, b) => { return a - b; });
                }
                if (expr.BinaryExpressionType == BinaryExpressionType.Multiply)
                {
                    if (expr.FirstExpression is IntegerLiteral && expr.SecondExpression is IntegerLiteral)
                        return calculate<IntegerLiteral, int>(expr, int.Parse, (a, b) => { return a * b; });
                    if (expr.FirstExpression is NumericLiteral && expr.SecondExpression is NumericLiteral)
                        return calculate<IntegerLiteral, float>(expr, float.Parse, (a, b) => { return a * b; });
                }
                if (expr.BinaryExpressionType == BinaryExpressionType.Divide)
                {
                    if (expr.FirstExpression is IntegerLiteral && expr.SecondExpression is IntegerLiteral)
                        return calculate<IntegerLiteral, int>(expr, int.Parse, (a, b) => { return a / b; });
                    if (expr.FirstExpression is NumericLiteral && expr.SecondExpression is NumericLiteral)
                        return calculate<IntegerLiteral, float>(expr, float.Parse, (a, b) => { return a / b; });
                }
            }
            return null;
        }
        T calculate<T, T1>(BinaryExpression val, Func<string, T1> parse, Func<T1, T1, T1> result)
             where T : Literal, new()
        {
            T res = new T();
            res.Value = result(parse((val.FirstExpression as Literal).Value), parse((val.SecondExpression as Literal).Value)).ToString();
            return res;
        }
        DeclareTableVariableBody getDeclareTableVariable(VariableTableReference vtr)
        {
            if (!tableVarible.ContainsKey(vtr.Variable.Name))
            {
                messages.addMessage(Code.T0000015, vtr.Variable.Name);
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
                return getLiteral(expression as VariableReference);
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
        private void getScalarSubquery(ScalarSubquery subquery)
        {
            if (subquery.QueryExpression is QuerySpecification)
            {
                var queryPart = subquery.QueryExpression as QuerySpecification;

                if (queryPart.TopRowFilter != null)
                {
                    var p = queryPart.TopRowFilter.Expression as ParenthesisExpression;
                    Literal literal = null;
                    if (p.Expression is VariableReference)
                    {
                        literal = getLiteral(p.Expression as VariableReference);
                    }
                    else
                    if (p.Expression is IntegerLiteral)
                    {
                        literal = p.Expression as IntegerLiteral;
                    }
                    if (literal == null)
                    {
                        messages.addMessage(Code.T0000036, p.Expression.GetType().Name);
                    }
                    else
                    {
                        if (int.Parse(literal.Value) != 1)
                        {
                            messages.addMessage(Code.T0000035, literal.Value);
                        }
                    }
                }
                else
                {
                    if(queryPart.FromClause != null)
                        messages.addMessage(Code.T0000034);
                }

                if (queryPart.WhereClause == null && queryPart.FromClause != null)
                {
                    messages.addMessage(Code.T0000037);
                }

                getQuerySpecification(queryPart);

                if (queryPart.SelectElements.Count > 1)
                {
                    messages.addMessage(Code.T0000012);
                }
                else
                if (queryPart.SelectElements.Count == 1)
                {
                    var el = queryPart.SelectElements[0];
                    if (el is SelectStarExpression)
                    {
                        messages.addMessage(Code.T0000013);
                        return;
                    }
                    if (el is SelectScalarExpression)
                    {
                        var expression = (el as SelectScalarExpression).Expression;
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
                            getScalarSubquery(expression as ScalarSubquery);
                        }
                    }
                }
            }
        }
        private Literal getConvertOrCast(ScalarExpression secondExpression)
        {
            dynamic castCall = secondExpression as CastCall;
            if (secondExpression is ConvertCall)
                castCall = secondExpression as ConvertCall;

            if (castCall.Parameter is VariableReference)
            {
                castCall.Parameter = getLiteral(castCall.Parameter as VariableReference);
            }

            StringLiteral sl = new StringLiteral();
            if (castCall.DataType is SqlDataTypeReference)
            {
                var DataType = castCall.DataType as SqlDataTypeReference;
                if (DataType.SqlDataTypeOption == SqlDataTypeOption.NVarChar || DataType.SqlDataTypeOption == SqlDataTypeOption.VarChar)
                {
                    if (castCall.Parameter is IntegerLiteral
                        //||
                        )
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
        BooleanExpression getBooleanBinaryExpression(BooleanExpression booleanExpression)
        {
            return new BooleanComparisonExpression();
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
                    checkColumnFromServer(firstColumn.Column, secondColumn.Column);
                }

                if (((firstColumn.Alias == null && secondColumn.Alias != null)
                     || (firstColumn.Alias != null && secondColumn.Alias == null))
                    && string.Compare(firstColumn.Name, secondColumn.Name, true) == 0)
                {
                    messages.addMessage(Code.T0000031, secondColumn.Name);
                }

                if (firstColumn.Alias != null && secondColumn.Alias != null)
                {
                    if (string.Compare(firstColumn.Alias, secondColumn.Alias, true) == 0
                    && string.Compare(firstColumn.Name, secondColumn.Name, true) == 0
                    )
                    {
                        messages.addMessage(Code.T0000032, firstColumn.Alias);
                    }
                    else
                    if (firstColumn.Alias != null && secondColumn.Alias != null
                        && string.Compare(firstColumn.Alias, secondColumn.Alias, true) == 0)
                    {
                        messages.addMessage(Code.T0000025, firstColumn.Alias);
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
                                messages.addMessage(Code.T0000020, firstColumn.FullName, search.ComparisonType.ToString());
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
        private MyColumn checkedColumnReference(ColumnReferenceExpression Expression)
        {
            var column = new MyColumn(null);
            var Identifiers = Expression.MultiPartIdentifier.Identifiers;
            column.Alias = Identifiers.Count > 1 ? Identifiers[Identifiers.Count - 2].Value : null;
            column.Name = Identifiers[Identifiers.Count - 1].Value;

            column.IsValid = !(Identifiers.Count > 2);

            if (!column.IsValid)
            {
                messages.addMessage(Code.T0000027, getNameColumn(Identifiers));
            }
            if (IsAliasAll && column.Alias == null)
            {
                messages.addMessage(Code.T0000019, column.Name);
            }
            if (column.IsValid)
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
                    messages.addMessage("типы для таблиц не равны!");
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
                    //    messages.addMessage(Code.T0000028, table.Name);
                    //}

                    column = table.Columns.SingleOrDefault(c => string.Compare(c.Name, firstname, true) == 0);
                    if (column == null)
                    {
                        messages.addMessage(Code.T0000029, firstname);
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
                    messages.addMessage(Code.T0000033, firstname);
                }
                messages.addMessage(Code.T0000030, firstname);
            }
            return column;
        }

        private TSqlFragment getTableFromAlias(string alias)
        {
            bool T = tables.ContainsKey(alias);
            bool W = withTables.ContainsKey(alias);
            if (!T && !W)
            {
                messages.addMessage(Code.T0000014, alias);
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

        private string getNameColumn(IList<Identifier> identifiers)
        {
            return string.Join(".", identifiers.Select(c => c.Value));
        }
        private string getNameTable(TableReference table)
        {
            if (table is NamedTableReference)
            {
                var named = table as NamedTableReference;
                return string.Join(".", named.SchemaObject.Identifiers.Select(c => c.Value));
            }
            return null;
        }

        private void checkeExpression(BooleanComparisonExpression search)
        {
            throw new NotImplementedException();
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
                return (table as QueryDerivedTable).Alias.Value;
            }
            return null;
        }

        private void checkedBooleanComparison(BooleanExpression booleanExpression)
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
        }

        private void checkedBooleanIsNullExpression(BooleanIsNullExpression booleanIsNullExpression)
        {
            if (booleanIsNullExpression.Expression is ColumnReferenceExpression)
            {
                var ex = booleanIsNullExpression.Expression as ColumnReferenceExpression;
                //проверить что поле может принимать значение null, иначе сравнение не корректно
                //messages.addMessage(Code.T0000021, getNameColumn(ex.MultiPartIdentifier.Identifiers));
            }
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
                messages.addMessage(Code.T0000017, key, getNameTable(tables[key].Obj));
                return;
            }
            tables.Add(key, new ReferCount<TableReference, int>(tableReference, 0));
        }
        public object CloneObject(object obj)
        {
            if (obj == null) return null;

            Type t1 = obj.GetType();
            object ret = Activator.CreateInstance(t1);

            var properties = t1.GetProperties().ToArray();
            for (int i = 0; i < properties.Length; i++)
            {
                if (properties[i].SetMethod == null) continue;
                properties[i].SetValue(
                        ret,
                        properties[i].GetValue(obj)
                    );
            }
            return ret;
        }
    }
}
