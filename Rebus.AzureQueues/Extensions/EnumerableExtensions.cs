using System.Collections.Generic;
using System.Linq;

namespace Rebus.Extensions
{
    static class EnumerableExtensions
    {
        public static IEnumerable<List<TItem>> Batch<TItem>(this IEnumerable<TItem> items, int batchSize)
        {
            var list = new List<TItem>();
            foreach (var item in items)
            {
                list.Add(item);
                if (list.Count >= batchSize)
                {
                    yield return list;
                    list = new List<TItem>();
                }
            }

            if (list.Any()) yield return list;
        }
    }
}