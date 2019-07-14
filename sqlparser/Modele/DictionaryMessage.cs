using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sqlparser.Modele
{
    public static class Code
    {
        public static string T0000001 { get; set; } = "Переменная '{0}' ни где не используется ";
        public static string T0000002 { get; set; } = "Длина меньше чем должна быть для переменной '{0}'. Входящие значение - {1}, указанное значение поля {2}";
        public static string T0000003 { get; set; } = "Переменная '{0}' уже объявлена";
        public static string T0000004 { get; set; } = "Переменная '{0}' или не объявлена или отсутствует параметр";
        public static string T0000005 { get; set; } = "Operand data type nvarchar is invalid for '{0}' operator.";
        public static string T0000006 { get; set; } = "System ParseError. Message: '{0}' Line: '{1}'";
        public static string T0000007 { get; set; } = "При вставки в таблицу '{0}' не указано полей";
        public static string T0000008 { get; set; } = "При вставки в таблицу '{0}' в качестве ресурса не рекомендуется использовать 'select *'";
        public static string T0000009 { get; set; } = "В выборке указано больше полей чем в целевой таблице '{0}'";
        public static string T0000010 { get; set; } = "В выборке указано меньше полей чем в целевой таблице '{0}'";
        public static string T0000011 { get; set; } = "Табличная переменная '{0}' ни где не используется";
        public static string T0000012 { get; set; } = "При скалярной выборки не может быть указано несколько полей";
        public static string T0000013 { get; set; } = "При скалярной выборки не может быть указано 'select *'";
        public static string T0000014 { get; set; } = "Указанный алиас '{0}' не найден";
        public static string T0000015 { get; set; } = "Указанный табличная переменная '{0}' не объявлена";
        public static string T0000016 { get; set; } = "Указанный временная таблица '{0}' не объявлена";
        public static string T0000017 { get; set; } = "Указанный алиас '{0}' уже существует для таблицы '{1}'";
        public static string T0000018 { get; set; } = "Указанный название для with '{0}' уже существует";
        public static string T0000019 { get; set; } = "Так как у всех таблиц есть алиас, то для указанного поля '{0}' тоже следует указать алиас";
        public static string T0000020 { get; set; } = "При сравнении поля '{0}' с NULL указано '{1}', а должно быть IS NULL";
        public static string T0000021 { get; set; } = "Поле '{0}' не принимает значение NULL условие не корректно";
        public static string T0000022 { get; set; } = "С сервера по объекту '{0}' ожидалось получить таблиц, но получен тип '{1}'";
        public static string T0000023 { get; set; } = "Обект '{0}' с сервера не получен, возможно отсутствует";
        public static string T0000024 { get; set; } = "Ошбики при обращении к серверу по таблице '{1}': '{0}'";
        public static string T0000025 { get; set; } = "Вероятно ошибка указания сравнения одной и той же таблицы - '{0}'";
        public static string T0000026 { get; set; } = "Вероятно ошибка указания сравнения одной и той же колонки - '{0}'";
        public static string T0000027 { get; set; } = "Ссылка на таблицу не может быть более чем из 2 частей - '{0}'";
        public static string T0000028 { get; set; } = "Таблицы '{0}' на сервере не существует";
        public static string T0000029 { get; set; } = "Указанного поля '{0}' на сервере не существует";
        public static string T0000030 { get; set; } = "Указанное поле '{0}' без алиас не найдено ни в одной из используемых таблиц";
        public static string T0000031 { get; set; } = "При сравнении не указан алиас для одного из полей '{0}'";
        public static string T0000032 { get; set; } = "Сравнение одного и того же поля '{0}'";
        public static string T0000033 { get; set; } = "Указанное поля '{0}' существует в более чем одной таблице";
        public static string T0000034 { get; set; } = "При скалярной выборке желательно указать TOP 1";
        public static string T0000035 { get; set; } = "При скалярной выборке в аргументе TOP должно быть указано одно значение, а указано '{0}'";
        public static string T0000036 { get; set; } = "При скалярной выборке в аргументе TOP указан тип '{0}' вместо целого числа ";
        public static string T0000037 { get; set; } = "При скалярной выборке отсутствует условие";
        public static string T0000038 { get; set; } = "Табличная переменная {0} уже объявлена";
        public static string T0000039 { get; set; } = "Временная таблица {0} уже была создана";
        public static string T0000040 { get; set; } = "Таблица {0} создается повторно";
        public static string T0000041 { get; set; } = "Перед созданием новой таблицы {0} отсутствует удаление";
        public static string T0000042 { get; set; } = "Все параметры для функции object_id должны быть текстовыми";
        public static string T0000043 { get; set; } = "Не удалось получить идентификатор из функции object_id параметра - '{0}' ";
        public static string T0000044 { get; set; } = "При удалении объекта '{0}' отсутствует проверка на существование ";
        public static string T0000045 { get; set; } = "Параметр '{0}' нигде не используется";
        public static string T0000046 { get; internal set; } = "Типы для таблиц не равны!";
        public static string T0000047 { get; internal set; } = "Тип переменной и присваемого значения не равны, тип переменной - {0}, а тип входящего значения {1}";
    }
}
