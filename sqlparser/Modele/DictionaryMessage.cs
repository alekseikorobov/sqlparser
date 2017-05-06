﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sqlparser.Modele
{
    public enum Code
    {
         T0000001
        ,T0000002
        ,T0000003
        ,T0000004
        ,T0000005
        ,T0000006
        ,T0000007
        ,T0000008
        ,T0000009
        ,T0000010
        ,T0000011
        ,T0000012
        ,T0000013,
        T0000014,
        T0000015,
        T0000016
    }
    public static class DictionaryMessage
    {
        static Dictionary<Code, string> data = new Dictionary<Code, string>()
        {
              { Code.T0000001,"Переменная {0} ни где не используется " }
            , { Code.T0000002,"Длина меньше чем должна быть для переменной {0}" }
            , { Code.T0000003,"Переменная {0} уже объявлена" }
            , { Code.T0000004,"В списке переменная {0} отсутствует"}
            , { Code.T0000005,"Operand data type nvarchar is invalid for {0} operator."}
            , { Code.T0000006,"System ParseError. Message: {0} Line: {1}" }
            , { Code.T0000007,"При вставки в таблицу {0} не указано полей" }
            , { Code.T0000008,"При вставки в таблицу {0} в качестве ресурса не рекомендуется использовать 'select *'" }
            , { Code.T0000009,"В выборке указано больше полей чем в целевой таблице {0}" }
            , { Code.T0000010,"В выборке указано меньше полей чем в целевой таблице {0}" }
            , { Code.T0000011,"Табличная переменная '{0}' ни где не используется" }
            , { Code.T0000012,"При скалярной выборки не может быть указано несколько полей" }
            , { Code.T0000013,"При скалярной выборки не может быть указано 'select *'" }
            , { Code.T0000014,"Указанный алиас {0} не найден" }
            , { Code.T0000015,"Указанный табличная переменная {0} не объявлена" }
            , { Code.T0000016,"Указанный временная таблица {0} не объявлена" }

        };
        public static string GetMessage(Code Code)
        {
            return data[Code];
        }

        public static string setData(this string str, string[] data)
        {
            return string.Format(str, data);
        }
    }
}
