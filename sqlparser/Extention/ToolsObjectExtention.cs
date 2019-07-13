using System;
using System.Linq;

namespace sqlparser
{
    public static class ToolsObjectExtention
    {
        public static T CloneObject<T>(this T obj) where T : class
        {
            if (obj == null) return default;

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
            return ret as T;
        }
    }
}
