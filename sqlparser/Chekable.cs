using Microsoft.SqlServer.TransactSql.ScriptDom;
using sqlparser.Modele;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sqlparser
{
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
        public List<Message> Messages = new List<Message>();
        Dictionary<string, ReferCount<DeclareVariableElement, int>> varible
            = new Dictionary<string, ReferCount<DeclareVariableElement, int>>();

        Dictionary<string, ReferCount<CreateTableStatement, int>> tempTeble
            = new Dictionary<string, ReferCount<CreateTableStatement, int>>();

        Dictionary<string, ReferCount<DeclareTableVariableBody, int>> tableVarible
            = new Dictionary<string, ReferCount<DeclareTableVariableBody, int>>();

        List<NamedTableReference> tables = new List<NamedTableReference>();

        internal void clearObjectFromStatement()
        {
            tables.Clear();
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
            if (select.QueryExpression != null && select.QueryExpression is QuerySpecification)
            {
                var Query = select.QueryExpression as QuerySpecification;
                if (Query.FromClause != null)
                {
                    var from = Query.FromClause as FromClause;
                    foreach (TableReference tableReference in from.TableReferences)
                    {
                        if (tableReference is QualifiedJoin)
                        {
                            var join = tableReference as QualifiedJoin;
                            var first = join.FirstTableReference as NamedTableReference;
                            var second = join.SecondTableReference as NamedTableReference;

                            AddTable(first);
                            AddTable(second);

                            checkedSearchCondition(join.SearchCondition);
                        }
                        if (tableReference is NamedTableReference)
                        {
                            if (IsTempTable((tableReference as NamedTableReference).SchemaObject.BaseIdentifier.Value))
                            {
                                getTemptable((tableReference as NamedTableReference).SchemaObject.BaseIdentifier.Value);
                            }
                            AddTable(tableReference as NamedTableReference);
                        }
                        if (tableReference is VariableTableReference)
                        {
                            getDeclareTableVariable(tableReference as VariableTableReference);
                        }
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
                    checkedSearchCondition(Query.WhereClause.SearchCondition);
                }
            }
        }

        private void checkedSearchCondition(BooleanExpression searchCondition)
        {
            if (searchCondition is BooleanBinaryExpression)
            {
                var search = searchCondition as BooleanBinaryExpression;
                checkedBooleanComparison(search);
            }
            if (searchCondition is BooleanComparisonExpression)
            {
                var search = searchCondition as BooleanComparisonExpression;
                checkedBooleanComparison(search);
            }
        }
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

        private void checkeExpression(BooleanComparisonExpression search)
        {
            throw new NotImplementedException();
        }

        private void checkAliasTable(IList<Identifier> identifires)
        {
            if (!tables.Any(c => string.Compare(c.Alias.Value, identifires[0].Value, true) == 0))
            {
                addMessage(Code.T0000014, identifires[0].Value);
            }
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
        }
        void AddTable(NamedTableReference tableReference)
        {
            tables.Add(tableReference);

            //if (tableReference is NamedTableReference)
            //{
            //    var tb = tableReference as NamedTableReference;
            //    var SchemaObject = tb.SchemaObject;

            //    if (SchemaObject == null) return;


            //    string table = (SchemaObject.DatabaseIdentifier == null ? "" : SchemaObject.DatabaseIdentifier.Value + ".")
            //    + (SchemaObject.SchemaIdentifier == null ? "" : SchemaObject.SchemaIdentifier.Value + ".")
            //     + SchemaObject.BaseIdentifier.Value;
            //}
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
