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
    public class Table
    {
        public Table()
        {
            Columns = new List<Column>();
        }
        public Table(string name)
        {
            Name = name;
            Columns = new List<Column>();
        }
        public bool IsExists { get; set; }
        public string Name { get; set; }
        public List<Column> Columns { get; set; }
    }
    public class Message
    {
        public Message(Code code, string[] data)
        {
            this.Code = code;
            this.Data = data;
        }
        public string MessageInformation
        {
            get
            {
                return DictionaryMessage.GetMessage(Code).setData(Data);
            }
        }
        public Code Code { get; set; }
        public string[] Data { get; set; }
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
        void GetObjectFromServer(TSqlFragment type)
        {
            if (type is NamedTableReference)
            {
                var t = type as NamedTableReference;
                string fullNameTable = getNameTable(t);
                var table = t.SchemaObject.BaseIdentifier.Value;
                var schema = t.SchemaObject.SchemaIdentifier?.Value ?? this.schema;
                database = t.SchemaObject.DatabaseIdentifier?.Value ?? database;
                var str = $"Server[@Name='{serverName}']/Database[@Name='{database}']/Table[@Name='{table}' and @Schema='{schema}']";
                SqlSmoObject obj = null;
                try
                {
                    obj = server.GetSmoObject(new Urn(str));
                }
                catch (Exception ex)
                {
                    if (ex.InnerException is MissingObjectException)
                    {
                        addMessage(Code.T0000023, fullNameTable);
                    }
                    else
                    {
                        StringBuilder sb = new StringBuilder();
                        for (; ex != null; ex = ex.InnerException)
                        {
                            sb.AppendLine(ex.Message);
                        }
                        addMessage(Code.T0000024, sb.ToString(), fullNameTable);
                    }
                }
                if (obj != null)
                {
                    if (obj is TableViewTableTypeBase)
                    {
                        var tableServer = obj as TableViewTableTypeBase;
                        var temptable = new Table(tableServer.ToString()) { IsExists = true };
                        foreach (Column item in tableServer.Columns)
                        {
                            temptable.Columns.Add(item);
                        }
                        TableFromServer.Add(fullNameTable, temptable);
                    }
                    else
                    {
                        addMessage(Code.T0000022, fullNameTable, obj.GetType().Name);
                    }
                }
                else
                {
                    TableFromServer.Add(fullNameTable, new Table(fullNameTable) { IsExists = false });
                }

            }
        }

        Server server;
        public Chekable()
        {
            server = new Server();
            server.ConnectionContext.ConnectionString = "data source=(local);initial catalog=master;integrated security=True;application name=master;MultipleActiveResultSets=True";
            server.ConnectionContext.Connect();
            var conect = (server.ConnectionContext.SqlConnectionObject as SqlConnection);
            serverName = conect.DataSource;
            database = conect.Database;
            schema = "dbo";
            //var obj = s.GetSmoObject(new Urn($"Server[@Name='RUMSKR90AZ5WD']/Database[@Name='{database}']/Table[@Name='{table}' and @Schema='{schema}']"));
            //if(obj is TableViewTableTypeBase)
            //{
            //    var t = obj as TableViewTableTypeBase;
            //    var tab = new Table(t.Name);
            //    foreach(Column c in t.Columns)
            //    {
            //        tab.Columns.Add(c);
            //    }

            //    TableFromServer.Add(tab.ToString(), tab);
            //}
            //foreach (var db0 in s.Databases)
            //{

            //}
            //s.ConnectionContext.Disconnect();

            //Microsoft.SqlServer.Management.Sdk.Sfc.Urn  urn= new Microsoft.SqlServer.Management.Sdk.Sfc.Urn();
            //urn.
            //s.GetSmoObject()
            Database db = new Database(server, database);
            db.Refresh();
            foreach (StoredProcedure sp in db.StoredProcedures)
            {

            }
            foreach (UserDefinedFunction func in db.UserDefinedFunctions)
            {

            }
            foreach (UserDefinedFunction func in db.UserDefinedFunctions)
            {

            }
            foreach (View item in db.Views)
            {

            }
            foreach (var item in db.Schemas)
            {

            }
            //db.UserDefinedFunctions


            //foreach (TableViewTableTypeBase table in db.Tables)
            //{
            //    string tableName = table.Name;

            //    if(table.Name == "t")
            //    {
            //        Table t = new Table(table.Name);
            //        foreach (Column column in table.Columns)
            //        {
            //            t.Columns.Add(column);
            //        }
            //        TableFromServer.Add(table.ToString(), t);
            //    }
            //}
            //UserDefinedTableType d = new UserDefinedTableType(db,"name");
        }
        public List<Message> Messages = new List<Message>();
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

        public void addMessage(Code code, params string[] data)
        {
            Messages.Add(new Message(code, data));
        }
        public void CheckUsengTableVarible()
        {
            foreach (var table in tableVarible)
            {
                if (table.Value.Count == 0)
                {
                    addMessage(Code.T0000011, table.Key);
                }
            }
        }
        public void CheckUsingVariable()
        {
            foreach (var value in varible)
            {
                if (value.Value.Count == 0)
                {
                    addMessage(Code.T0000001, value.Value.Obj.VariableName.Value);
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
                        addMessage(Code.T0000002, var.VariableName.Value);
                    }
                }
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
            return getDeclare(variableReference).Value as Literal;
        }
        private DeclareVariableElement getDeclare(VariableReference variableReference)
        {
            if (string.IsNullOrEmpty(variableReference.Name))
            {
                throw new Exception(string.Format("Переменная не определена"));
            }
            if (!varible.ContainsKey(variableReference.Name))
            {
                addMessage(Code.T0000004, variableReference.Name);
            }
            varible[variableReference.Name].Count++;
            return varible[variableReference.Name].Obj;
        }
        CreateTableStatement getTemptable(string name)
        {
            if (!tempTeble.ContainsKey(name))
            {
                addMessage(Code.T0000016, name);
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
            if (statement.InsertSpecification.Target is NamedTableReference)
            {
                var Target = statement.InsertSpecification.Target as NamedTableReference;
                target = string.Join(".", Target.SchemaObject.Identifiers.Select(c => c.Value));
                columns = getSpecificationTableTarget(Target);
                isTargetValidate = columns != null;
            }
            if (statement.InsertSpecification.Target is VariableTableReference)
            {
                var body = getDeclareTableVariable(statement.InsertSpecification.Target as VariableTableReference);
                columns = body.Definition.ColumnDefinitions.ToList();
                target = body.VariableName.Value;
            }
            if (statement.InsertSpecification.Columns.Count == 0)
            {
                addMessage(Code.T0000007, target);
            }
            if (statement.InsertSpecification.InsertSource is SelectInsertSource)
            {
                var select = statement.InsertSpecification.InsertSource as SelectInsertSource;
                var query = select.Select as QuerySpecification;
                if (query.SelectElements.Count > statement.InsertSpecification.Columns.Count)
                {
                    addMessage(Code.T0000009, target);
                }
                else
                if (query.SelectElements.Count < statement.InsertSpecification.Columns.Count)
                {
                    addMessage(Code.T0000010, target);
                }

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
                        addMessage(Code.T0000008, target);
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
                    addMessage(Code.T0000003, declar.VariableName.Value);
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
                addMessage(Code.T0000018, key);
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
                    //if (IsAddTableList)
                    //{
                    //    AddTable(tableReference);
                    //}
                    //else
                    //{
                    //    AddTableFromWith(tableReference);
                    //}
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

        private Literal calculateExpression(BinaryExpression expression)
        {
            if (expression.FirstExpression is BinaryExpression)
            {
                expression.FirstExpression = calculateExpression(expression.FirstExpression as BinaryExpression);
            }

            expression.FirstExpression = convertExpression(expression.FirstExpression);

            expression.SecondExpression = convertExpression(expression.SecondExpression);

            if (expression.FirstExpression is StringLiteral && expression.SecondExpression is StringLiteral)
            {
                if (expression.BinaryExpressionType == BinaryExpressionType.Add)
                {
                    StringLiteral s = new StringLiteral();
                    s.Value = (expression.FirstExpression as StringLiteral).Value
                        + (expression.SecondExpression as StringLiteral).Value;
                    return s;
                }
                else
                {
                    addMessage(Code.T0000005, expression.BinaryExpressionType.ToString());
                }
            }
            return null;
        }
        DeclareTableVariableBody getDeclareTableVariable(VariableTableReference vtr)
        {
            if (!tableVarible.ContainsKey(vtr.Variable.Name))
            {
                addMessage(Code.T0000015, vtr.Variable.Name);
                return null;
            }
            tableVarible[vtr.Variable.Name].Count++;
            return tableVarible[vtr.Variable.Name].Obj;
        }
        private ScalarExpression convertExpression(ScalarExpression expression)
        {
            if (expression is StringLiteral)
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

                if (queryPart.SelectElements.Count > 1)
                {
                    addMessage(Code.T0000012);
                }
                else
                if (queryPart.SelectElements.Count == 1)
                {
                    if (queryPart.SelectElements[0] is SelectStarExpression)
                    {
                        addMessage(Code.T0000013);
                    }
                    if (queryPart.SelectElements[0] is SelectScalarExpression)
                    {
                        var expression = (queryPart.SelectElements[0] as SelectScalarExpression).Expression;
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
        private List<ColumnDefinition> getSpecificationTableTarget(NamedTableReference insertSpecification)
        {
#warning Надо научиться получать структуру таблицы;
            //new List<ColumnDefinition>()
            //insertSpecification.
            return null;
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
                var firstExpression = search.FirstExpression as ColumnReferenceExpression;
                var secondExpression = search.SecondExpression as ColumnReferenceExpression;

                var firstIdentifiers = firstExpression.MultiPartIdentifier.Identifiers;
                var secondIdentifiers = secondExpression.MultiPartIdentifier.Identifiers;

                //string f = firstIdentifiers[0].Value + "." + firstIdentifiers[1].Value;
                //string s = secondIdentifiers[0].Value + "." + secondIdentifiers[1].Value;
                //string gen = f + " " + search.ComparisonType.ToString() + " " + s;

                if (string.Compare(firstIdentifiers[0].Value
                                    , secondIdentifiers[0].Value
                                    , true) == 0
                        )
                {
                    throw new Exception("Вероятно ошибка указания сравнения одной и той же таблицы");
                }

#warning нужны проверки если алиас не был указан
                checkAliasTable(firstIdentifiers);
                checkAliasTable(secondIdentifiers);

            }
            else
            {
                if (search.FirstExpression is ColumnReferenceExpression)
                {
                    var firstExpressionColumn = search.FirstExpression as ColumnReferenceExpression;
                    checkAliasTable(firstExpressionColumn.MultiPartIdentifier.Identifiers);
                    if (search.SecondExpression is VariableReference)
                    {

                    }
                    if (search.SecondExpression is Literal)
                    {
                        if (search.SecondExpression is NullLiteral)
                        {
                            if (search.ComparisonType == BooleanComparisonType.Equals)
                            {
                                addMessage(Code.T0000020, getNameColumn(firstExpressionColumn.MultiPartIdentifier.Identifiers), search.ComparisonType.ToString());
                            }
                        }
                    }
                }
                else if (search.SecondExpression is ColumnReferenceExpression)
                {
                    var firstExpressionColumn = search.SecondExpression as ColumnReferenceExpression;
                    checkAliasTable(firstExpressionColumn.MultiPartIdentifier.Identifiers);
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

        private string getNameColumn(IList<Identifier> identifiers)
        {
            return string.Join(".", identifiers.Select(c => c.Value));
        }

        private void checkeExpression(BooleanComparisonExpression search)
        {
            throw new NotImplementedException();
        }

        private void checkAliasTable(IList<Identifier> identifires)
        {
            if (IsAliasAll && identifires.Count == 1)
            {
                //Если у всех таблиц есть алиас, то проверить, в выборке есть ли поля без алиас, если да, то вывести сообщение
                addMessage(Code.T0000019, identifires[0].Value);
            }
            if (!tables.ContainsKey(identifires[0].Value) && !withTables.ContainsKey(identifires[0].Value))
            {
                addMessage(Code.T0000014, identifires[0].Value);
            }
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
        private string getNameTable(TableReference table)
        {
            if (table is NamedTableReference)
            {
                var named = table as NamedTableReference;
                return string.Join(".", named.SchemaObject.Identifiers.Select(c => c.Value));
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
                //addMessage(Code.T0000021, getNameColumn(ex.MultiPartIdentifier.Identifiers));
            }
        }

        bool IsAliasAll = true;
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
                addMessage(Code.T0000017, key, getNameTable(tables[key].Obj));
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
