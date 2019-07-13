namespace sqlparser
{
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
}
