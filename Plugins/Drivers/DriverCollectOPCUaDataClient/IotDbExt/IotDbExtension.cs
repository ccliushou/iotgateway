using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json.Nodes;
using System.Text.Json;
using System.Text.RegularExpressions;
using Salvini.IoTDB;
using Silkier;
using Salvini.IoTDB.Data;

namespace DriverCollectOPCUaDataClient.IotDbExt
{
    public static class IotDbExtension
    {

        public static string ReplaceNodeIdStr(this string nodeID)
        {
            return nodeID.Replace("&quot;", "\"");

        }
        public static bool CheckDataBaseOpen(this Session _session)
        {
            bool dbisok = false;

            if (!_session.IsOpen)
            {
                dbisok = Retry.RetryOnAny(10, f =>
                {
                    _session.OpenAsync().Wait(TimeSpan.FromMilliseconds(100));
                    return true;
                }, ef =>
                {
                    // _logger.LogError();
                    Console.WriteLine($" open iotdb error。{ef.current}次失败{ef.ex.Message} {ef.ex.InnerException?.Message}");
                });
            }else
            {
                dbisok = true;
            }
            return dbisok;
        }

        /// <summary>
        /// 查询设备的测点信息
        /// </summary>
        /// <param name="session"></param>
        /// <param name="device">设备名称，不含root</param>
        /// <param name="keywords"></param>
        /// <returns></returns>
        public static async Task<List<(string Tag, string Type, string Desc, string Unit, double? Downlimit, double? Uplimit, DateTime? modifyTime)>>
            PointsAsync(this Session session,string device, string keywords = "")
        {
            if (keywords?.StartsWith("/") == true) keywords = keywords[1..];
            if (keywords?.EndsWith("/") == true) keywords = keywords[0..^1];
            if (keywords?.EndsWith("/i") == true) keywords = keywords[0..^2];
            static JsonObject DeserializeObject(string json)
            {
                if (!string.IsNullOrEmpty(json) && json != "NULL")
                {
                    try
                    {
                        return JsonSerializer.Deserialize<JsonObject>(json.Replace("\\", "/"));
                    }
                    catch (System.Exception ex)
                    {

                    }
                }
                return new JsonObject();
            }
            using var query = await session.ExecuteQueryStatementAsync($"show timeseries root.{device}");
            var points = new List<(string Tag, string Type, string Desc, string Unit, double? Downlimit, double? Uplimit, DateTime? @modifyTime)>();
            var len = device.Length + 6;
            var reg = new Regex(keywords ?? "", RegexOptions.IgnoreCase);
            while (query.HasNext())
            {
                var next = query.Next();
                var values = next.Values;
                var id = ((string)values[0])[len..];
                if (!reg.IsMatch(id)) continue;
                var tags = DeserializeObject((string)values[^2]);
                var attributes = DeserializeObject((string)values[^1]);
                points.Add((id, (string)tags?["t"], (string)tags?["d"], (string)tags?["u"], double.TryParse((string)attributes?["l"], out var l) ? l : default(double?), double.TryParse((string)attributes?["h"], out var h) ? h : default(double?), DateTime.TryParse((string)tags?["@t"], out var modify) ? modify : default(DateTime?)));
            }
            return points.OrderBy(x => x.Tag).ToList();
        }




        /// <summary>
        /// 查询设备的最新一条数据
        /// </summary>
        /// <param name="session"></param>
        /// <param name="device"></param>
        /// <param name="tags"></param>
        /// <returns></returns>
        public static async Task<List<(string Tag, DateTime Time, double Value)>> SnapshotAsync(this Session session, string device, List<string> tags)
        {
            var len = device.Length + 6;
            var sql = $"select last {string.Join(",", tags)} from root.{device}";
            using var query = await session.ExecuteQueryStatementAsync(sql);
            var data = new List<(string Tag, DateTime Time, double Value)>();
            while (query.HasNext())
            {
                var next = query.Next();
                var values = next.Values;
                var id = ((string)values[0])[len..];
                var time = next.GetDateTime();
                var value = values[1] == null ? double.NaN : double.Parse((string)values[1]);
                data.Add((id, time, value));
            }
            return data;
        }

        /// <summary>
        /// 查询设备的一段历史数据
        /// </summary>
        /// <param name="session"></param>
        /// <param name="device"></param>
        /// <param name="tag"></param>
        /// <param name="begin"></param>
        /// <param name="end"></param>
        /// <param name="digits"></param>
        /// <returns></returns>
        public static async Task<List<(DateTime Time, double Value)>> ArchiveAsync(this Session session, string device, string tag, DateTime begin, DateTime end, int digits = 6)
        {
            var sql = $"select {tag} from root.{device} where time>={begin:yyyy-MM-dd HH:mm:ss.fff} and time<={end:yyyy-MM-dd HH:mm:ss.fff} align by device";
            using var query = await session.ExecuteQueryStatementAsync(sql);
            var data = new List<(DateTime Time, double Value)>();
            while (query.HasNext())
            {
                var next = query.Next();
                var values = next.Values;
                var time = next.GetDateTime();
                var value = "NULL".Equals(values[1]) ? double.NaN : (double)values[1];
                data.Add((time, value));
            }
            return data;
        }

        public static async Task<List<(DateTime Time, double Value)>> HistoryAsync(this Session session, string device, string tag, DateTime begin, DateTime end, int digits = 6, int ms = 1000)
        {
            var @break = false;
            var data = new List<(DateTime Time, double Value)>();
            if ((end - begin).TotalHours > 4)//4小时以内可以不检测数据是否存在
            {
                var sql = $"select count({tag}) as exist from root.{device} where time >= {begin:yyyy-MM-dd HH:mm:ss} and time < {end:yyyy-MM-dd HH:mm:ss}";
                using var query = await session.ExecuteQueryStatementAsync(sql);
                @break = query.HasNext() && (long)query.Next().Values[0] == 0;
            }
            if (!@break)
            {
                var sql = $"select last_value({tag}) as {tag} from root.{device} group by ([{begin:yyyy-MM-dd HH:mm:ss},{end.AddMilliseconds(ms):yyyy-MM-dd HH:mm:ss}), {ms}ms) fill(double[previous])";
                using var query = await session.ExecuteQueryStatementAsync(sql);
                while (query.HasNext())
                {
                    var next = query.Next();
                    var values = next.Values;
                    var time = next.GetDateTime();
                    var value = "NULL".Equals(values[0]) ? double.NaN : (double)values[0];
                    data.Add((time, value));
                }
            }
            return data;

        }
        /// <summary>
        /// 批量插入数据
        /// </summary>
        /// <param name="session"></param>
        /// <param name="device"></param>
        /// <param name="matrix"></param>
        /// <returns></returns>
        public static async Task BulkWriteAsync(this Session session, string device, dynamic[,] matrix)
        {
            var rows = matrix.GetUpperBound(0) + 1;
            var columns = matrix.GetUpperBound(1) + 1;
            if (rows != 0)
            {
                var cols = Enumerable.Range(1, columns - 1).ToList();
                var measurements = cols.Select((j) => ((string)matrix[0, j]).Replace("root.", string.Empty)).ToList();
                if (rows == 2)
                {
                    var values = cols.Select((j) => matrix[1, j]).ToList();
                    var record = new RowRecord(UTC_MS(matrix[1, 0]), values, measurements);
                    try
                    {

                        var effect = await session.InsertRecordAsync($"root.{device}", record, false);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($" BulkWriteAsync iotdb error。{ex.Message} {ex.InnerException?.Message} {ex.StackTrace}");

                       // throw;
                    }

                }
                else
                {
                    var timestamps = new List<DateTime>();
                    var values = new List<List<dynamic>>();
                    for (var i = 1; i < rows; i++)
                    {
                        timestamps.Add(matrix[i, 0]);
                        values.Add(cols.Select(j => matrix[i, j]).ToList());
                    }
                    var tablet = new Tablet($"root.{device}", measurements, values, timestamps);
                    var effect = await session.InsertTabletAsync(tablet, false);
                }
            }
        }



        /// <summary>
        /// 多测点单时刻数据
        /// </summary>
        /// <param name="device">所属设备或数据库</param>
        /// <param name="time">时间戳</param> 
        /// <param name="data">测点数据</param>
        public static async Task BulkWriteAsync(this Session session,string device, DateTime time, List<(string Tag, object Value)> data)
        {
            var matrix = new dynamic[2, data.Count + 1];
            matrix[0, 0] = "Timestamp";
            matrix[1, 0] = time;
            for (int j = 0; j < data.Count; j++)
            {
                matrix[0, j + 1] = data[j].Tag;
                matrix[1, j + 1] = data[j].Value;
            }
            await session.BulkWriteAsync(device, matrix);
        }




        private readonly static DateTime __UTC_TICKS__ = new DateTime(1970, 01, 01).Add(TimeZoneInfo.Local.BaseUtcOffset);
        private static long UTC_MS(DateTime time)
        {
           return (long)(time - __UTC_TICKS__).TotalMilliseconds;
        }
        private static string IotDataTypeStr(Type Type)
        {
            if (Type == typeof(string) ||
                Type == typeof(byte[]) ||
                Type == typeof(short[]) ||
                Type == typeof(int[]) ||
                Type == typeof(double[]) ||
                Type == typeof(float[]) ||
                Type == typeof(ushort[]))
            {
                return "TEXT";
            }
            else if (Type == typeof(int) ||
                Type == typeof(uint) ||
                Type == typeof(short) ||
                Type == typeof(byte) ||
                Type == typeof(ushort))
            {

                return "INT32";
            }
            else if (Type == typeof(Int64) ||
                    Type == typeof(long) )
            {
                return "INT64";
            }
            else if (Type == typeof(float) ||
                    Type == typeof(Single))
            {

                return "FLOAT";
            }
            else if (Type == typeof(double))
            {

                return "DOUBLE";
            }
            else if (Type == typeof(bool))
            {
                return "BOOLEAN";
            }
            return "TEXT";
        }
        public static async Task InitializeAsync(this Session session, string shortdeviceName, List<(string Tag, Type Type, string Desc, string Unit, double? Downlimit, double? Uplimit)> measurements)
        {
            var exist = await session.PointsAsync(shortdeviceName);

            //var exist = new List<(string Tag, string Type, string Desc, string Unit, double? Downlimit, double? Uplimit, DateTime? modifyTime)>();
            foreach (var point in measurements)
            {
                var _id = point.Tag;
                string sql;
                if (exist.Any(x => x.Tag == _id && x.Type == point.Type.ToString()))
                {
                    //已经存在的时间序列。
                    sql = $"alter timeseries root.{shortdeviceName}.{_id} upsert tags (t='{point.Type}', u='{point.Unit}', d='{point.Desc}', @t='{DateTime.Now:yyyy-MM-dd HH:mm:ss}')";
                }
                else
                {// 时间序列的数据类型如果发生变化呢？？？
                    string iotDataType = IotDataTypeStr(point.Type);
                    sql = $"create timeseries root.{shortdeviceName}.{_id} with datatype={iotDataType} tags ( t='{point.Type}', u='{point.Unit}', d='{point.Desc}', @t='{DateTime.Now:yyyy-MM-dd HH:mm:ss}')";
                }
                if (point.Downlimit != null && point.Uplimit != null)
                {
                    sql += $" attributes (l='{point.Downlimit}',h='{point.Uplimit}')";
                }
                var effect = await session.ExecuteNonQueryStatementAsync(sql);
                Console.WriteLine($"iotdb:InitializeAsync:{effect}>>{sql}");
                //Console.WriteLine($"\n{sql}");
            }
        }



    }
}
