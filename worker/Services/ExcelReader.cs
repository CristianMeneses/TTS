using System.Data;
using ExcelDataReader;

namespace TtsWorker.Services;

static class ExcelReader
{
    static ExcelReader()
    {
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
    }

    public static List<Dictionary<string, string>> ReadRows(byte[] fileBytes, string fileName)
    {
        using var stream = new MemoryStream(fileBytes);

        IExcelDataReader reader = Path.GetExtension(fileName).ToLowerInvariant() == ".csv"
            ? ExcelReaderFactory.CreateCsvReader(stream)
            : ExcelReaderFactory.CreateReader(stream);

        var dataset = reader.AsDataSet(new ExcelDataSetConfiguration
        {
            ConfigureDataTable = _ => new ExcelDataTableConfiguration { UseHeaderRow = true }
        });

        var table = dataset.Tables[0];
        var rows = new List<Dictionary<string, string>>(table.Rows.Count);

        var headers = table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();

        foreach (DataRow row in table.Rows)
        {
            var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var header in headers)
                dict[header] = row[header]?.ToString() ?? "";
            rows.Add(dict);
        }

        return rows;
    }

    public static IEnumerable<List<T>> Chunk<T>(List<T> source, int size)
    {
        for (int i = 0; i < source.Count; i += size)
            yield return source.GetRange(i, Math.Min(size, source.Count - i));
    }
}
