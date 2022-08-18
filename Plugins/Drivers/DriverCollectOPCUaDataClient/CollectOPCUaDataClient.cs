using DriverCollectOPCUaDataClient.IotDbExt;
using Opc.Ua;
using Opc.Ua.Client;
using OpcUaHelper;
using PluginInterface;
using Silkier.Extensions;
using System.Text;
using System.Text.RegularExpressions;
using NPOI.HSSF.Util;
using NPOI.SS.UserModel;
using NPOI.SS.Util;
using NPOI.XSSF.UserModel;
using NPOI.HSSF.UserModel;

namespace DriverCollectOPCUaDataClient
{
    /// <summary>
    /// 采集策略
    /// </summary>
    enum CollectionStrategyEnum
    {
        不采集,订阅,轮询

    }
    class CollectionStrategy
    {
        //数据名称	数据	类型	备注	采集策略
        public string 数据名称 { get; set; }
        public string 数据 { get; set; }
        public string 类型 { get; set; }
        public string 备注 { get; set; }
        public CollectionStrategyEnum 采集策略 { get; set; }

        public override string ToString()
        {
            return $"{数据名称},{数据},{类型},{备注},{采集策略}";
        }
    }
    [DriverSupported("OPC UA && IotDB")]
    [DriverInfoAttribute("CollectOPCUaDataClient", "V0.1.1", "Copyright ccliushou 2022-8-16")]
    public class CollectOPCUaDataClient : IDriver
    {
        OpcUaClientHelper opcUaClient = null;
        Salvini.IoTDB.Session _iotclient = null;

        #region 配置参数

        [ConfigParameter("设备Id")]
        public Guid DeviceId { get; set; }

        [ConfigParameter("uri")]
        public string Uri { get; set; } = "opc.tcp://localhost:62541/Quickstarts/ReferenceServer";

        [ConfigParameter("超时时间ms")]
        public int Timeout { get; set; } = 3000;

        [ConfigParameter("最小通讯周期ms")]
        public uint MinPeriod { get; set; } = 3000;


        [ConfigParameter("变量定义文件")]
        public string OpcVariableExcelFile { get; set; } = @"灌装机数据整理@D:\融成智造\客户数据\864\配置信息\数据采集修改-863-变量.xlsx";


        [ConfigParameter("IotDB 地址")]
        public string IotDBUrl { get; set; } = @"iotdb://root:root@127.0.0.1:6667/?appName=iTSDB&fetchSize=1800";// @"iotdb://root:root@127.0.0.1:6667/appName=testapp"   @"server=127.0.0.1;user=root;password=root;database=IoTSharp"

        [ConfigParameter("IotDb存储组名称STORAGE group")]
        public string StorageGroupName { get; set; } = "rczz";

        [ConfigParameter("一次轮询的最大测点数量")]
        public int PLCPointMaxCount { get; set; } = 200;

        [ConfigParameter("NodeIdFile")]
        public string NodeIdNsPrefix { get; set; } = "ns=3;s=";

        [ConfigParameter("NodeIdParentName")]
        public string NodeIdParentName { get; set; } = "工单发送_DB";

        [ConfigParameter("NodeIdFile")]
        public string NodeIdFile { get; set; } = String.Empty;


        [ConfigParameter("第一组NodeId")]
        public string TopNodeId_1 { get; set; } = "数据1|ns=2;s=实时模拟";


        [ConfigParameter("是否统计变量信息")]
        public bool IsReportVarsInfor { get; set; } = false;

        #endregion

        #region 

        /// <summary>
        /// key 为 deviceShortName，value为所属的测点信息
        /// </summary>
        private Dictionary<string, List<ReferenceDescription>> OpcVariableNodeDic = new Dictionary<string, List<ReferenceDescription>>();//测点

        /// <summary>
        /// key 为deviceShortName,value为时间序列信息
        /// </summary>
        private Dictionary<string, List<(string Tag, Type Type, string Desc, string Unit, double? Downlimit, double? Uplimit)>> IotDBMeasureList = new Dictionary<string, List<(string Tag, Type Type, string Desc, string Unit, double? Downlimit, double? Uplimit)>>();
        #endregion

        public CollectOPCUaDataClient(Guid deviceid)
        {
            DeviceId = deviceid;
        }

        public bool IsConnected
        {
            get
            {
                return opcUaClient != null && opcUaClient.Connected;
            }
        }
        private List<ReferenceDescription> GetBrowseVariableNodeList(string tag)
        {
            if (opcUaClient == null || string.IsNullOrEmpty(tag))
                return null;
            ReferenceDescription[] NodeList = opcUaClient.BrowseNodeReference2(tag);
            if (NodeList == null || NodeList.Length == 0)
                NodeList = opcUaClient.BrowseNodeReference(tag);

            var nodeDescriptionList = NodeList.Where(item =>
            {
                return (item.NodeClass == NodeClass.Variable);
            }).ToList();
            return nodeDescriptionList;
        }
        private void SubscripCallBack(string key, MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            MonitoredItemNotification monitoredItemNotification = e.NotificationValue as MonitoredItemNotification;
            if (monitoredItemNotification != null)
            {

                string[] tagInfor = key.Split("|");

                var deviceShortNameByStorageGroupName = tagInfor[0];
                var measurePointInfor = tagInfor[1];
                ReferenceDescription referenceDescription = new ReferenceDescription() { DisplayName = new LocalizedText(measurePointInfor) };

                DateTime ts = monitoredItemNotification.Message.PublishTime.AddHours(8);//转为北京时间


                var res = SaveNodeDataValue2IotDb(deviceShortNameByStorageGroupName,
                                                    ts,
                                                    new List<DataValue>() { monitoredItemNotification.Value },
                                                    new List<ReferenceDescription>() { referenceDescription }).Result;

                Console.WriteLine($"{key},{item.ToString()},{monitoredItemNotification.Value.Value.ToString()},数据保存结果:{res}");

            }
        }
        private void TestSubscription()
        {
            //opcUaClient.AddSubscription("testSub", SubscriptionNodeId, SubscripCallBack);
        }
        private void InitNodeList(string TopTag)
        {
            if (string.IsNullOrEmpty(TopTag))
                return;

            string[] tasInfor = TopTag.Split("|");
            if (tasInfor.Length != 2)
                return;

            var NodeList = GetBrowseVariableNodeList(tasInfor[1]);
            var deviceShortNameByStorageGroupName = string.Format("{0}.{1}", StorageGroupName, tasInfor[0]);

            InitOpcDriverNodeList(NodeList, deviceShortNameByStorageGroupName);

        }

        private List<CollectionStrategy> ExcelVarList = new List<CollectionStrategy>();
        private void LoadExcelVarData()
        {
            ExcelVarList.Clear();
            //数据名称	数据	类型	备注	采集策略

            if (OpcVariableExcelFile.IsNullOrEmpty())
                return;
           var sheetName= OpcVariableExcelFile.Split('@')[0];
            var excelFile = OpcVariableExcelFile.Split('@')[1];
            if (!File.Exists(excelFile))
                return;
            //CollectionStrategy


            using (FileStream fs = new FileStream(excelFile, FileMode.Open))
            {

                XSSFWorkbook workbook = new XSSFWorkbook(fs);
                ISheet sheet = workbook.GetSheet(sheetName);
                int rfirst = sheet.FirstRowNum;
                int rlast = sheet.LastRowNum;
                IRow row = sheet.GetRow(rfirst);

                var 数据名称Index= row.Where(c => c.RichStringCellValue.String.Equals("数据名称")).Select(c=>c.ColumnIndex).FirstOrDefault();
                var 数据Index = row.Where(c => c.RichStringCellValue.String.Equals("数据")).Select(c => c.ColumnIndex).FirstOrDefault();

                var 类型Index = row.Where(c => c.RichStringCellValue.String.Equals("类型")).Select(c => c.ColumnIndex).FirstOrDefault();
                var 备注Index = row.Where(c => c.RichStringCellValue.String.Equals("备注")).Select(c => c.ColumnIndex).FirstOrDefault();
                var 采集策略Index = row.Where(c => c.RichStringCellValue.String.Equals("采集策略")).Select(c => c.ColumnIndex).FirstOrDefault();


                int cfirst = row.FirstCellNum;
                int clast = row.LastCellNum;
                //数据名称	数据	类型	备注	采集策略
                for (int i = rfirst + 1; i <= rlast; i++)
                {
                    CollectionStrategy collectionStrategy = new CollectionStrategy();
                    IRow ir = sheet.GetRow(i);
                    var c = ir.GetCell(采集策略Index);
                    if (c == null)
                        break;
                    collectionStrategy.数据名称 = ir.GetCell(数据名称Index)?.RichStringCellValue.String;
                    collectionStrategy.数据 = ir.GetCell(数据Index)?.RichStringCellValue.String;
                    collectionStrategy.类型 = ir.GetCell(类型Index)?.RichStringCellValue.String;
                    collectionStrategy.备注 = ir.GetCell(备注Index)?.RichStringCellValue.String;
                    collectionStrategy.采集策略 = c.RichStringCellValue.String.ToEnum<CollectionStrategyEnum>();
                    ExcelVarList.Add(collectionStrategy);


                }
                sheet = null;
                workbook = null;
            }
        }

        /// <summary>
        /// 获取node的采集策略
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        private CollectionStrategy GetNodeCollectionStrategyByExcel(ReferenceDescription node)
        {
            var item= ExcelVarList.Where(e => e.数据名称 == node.DisplayName.Text || e.数据 == node.DisplayName.Text).FirstOrDefault();
            if (item == null)
                return null;
            else
                return item;
        }
        private async void InitOpcDriverNodeList(List<ReferenceDescription> NodeList,string deviceShortNameByStorageGroupName)
        {
            if(IsReportVarsInfor)
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendLine("序号,DisplayName,BrowseName,数据名称,数据,类型,备注,采集策略,统计情况");
                int index = 0;
                //查询该设备的测点信息
                //var iotdbPoints = await _iotclient.PointsAsync(deviceShortNameByStorageGroupName);
                foreach (var node in NodeList)
                {

                    var temp_Strategy = GetNodeCollectionStrategyByExcel(node);
                    string tempStr = "-,-,-,-,-";
                    if (temp_Strategy != null)
                    {
                        tempStr = temp_Strategy.ToString();
                    }
                    string iotPointInforStr = "-";
                    var iotPointInfor =await _iotclient.QueryDistinctAsync(deviceShortNameByStorageGroupName,node.DisplayName.Text,DateTime.Now.AddDays(-7),DateTime.Now );
                    if(iotPointInfor!=null&& iotPointInfor.Count>0)
                    {
                        iotPointInforStr = "";
                        int showCount = iotPointInfor.Count;
                        if (showCount > 5)
                            showCount = 5;
                        for (int i = 0; i < showCount; i++)
                        {
                            iotPointInforStr = iotPointInforStr + $"|{iotPointInfor[i].Value}";
                        }
                        iotPointInforStr = $"【{iotPointInfor.Count}】" + iotPointInforStr;
                    }
                    sb.AppendLine($"{++index},{node.DisplayName.Text},{node.BrowseName.ToString()},{tempStr},{iotPointInforStr}");
                }
                string filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Drivers", "ReadDevVars");
                if (Directory.Exists(filePath))
                {
                    Directory.Delete(filePath, true);
                }

                Directory.CreateDirectory(filePath);
                string fileName = Path.Combine(filePath, $"OPC-device-{deviceShortNameByStorageGroupName}.csv");
                File.WriteAllText(fileName, sb.ToString(), Encoding.UTF8);
            }

            var iotFulldeviceName = string.Format("root.{0}", deviceShortNameByStorageGroupName);
            List<(string Tag, Type Type, string Desc, string Unit, double? Downlimit, double? Uplimit)> measurements = new List<(string Tag, Type Type, string Desc, string Unit, double? Downlimit, double? Uplimit)>();
            

            //订阅的节点
            List<ReferenceDescription> referencesSubscriptionList = new List<ReferenceDescription>();
            //轮询的节点
            List<ReferenceDescription> referencesList = new List<ReferenceDescription>();
            NodeList.ForEach(node =>
            {
                //与excel中的配置的采集策略进行对比：

                var nodeCollectionStrategy = GetNodeCollectionStrategyByExcel(node);

                if (nodeCollectionStrategy==null||nodeCollectionStrategy.采集策略 == CollectionStrategyEnum.不采集)
                {
                    return;
                }

                var dataValue = opcUaClient.ReadNode(new NodeId(node.NodeId.ToString()));//读取一次
                if (DataValue.IsGood(dataValue))
                {
                    if (dataValue.Value == null)
                    {

                    }
                    else
                    {

                        if (dataValue.GetType() == typeof(NodeId))
                        {
                            //Opc.Ua.IdType
                            //NodeId t =(NodeId)dataValue ;

                        }else if (dataValue.Value.GetType()== typeof(string))
                        {

                        }
                        else
                        {//
                            //measurements.Add(("Latitude", "double", "gps Latitude", null, null, null));
                            measurements.Add((node.DisplayName.Text, dataValue.Value.GetType(), node.BrowseName.ToString(), null, null, null));
                            if (nodeCollectionStrategy.采集策略 == CollectionStrategyEnum.轮询)
                                referencesList.Add(node);
                            else
                                referencesSubscriptionList.Add(node);

                        }
                            
                    }
                }
            });

            //
            await _iotclient.InitializeAsync(deviceShortNameByStorageGroupName, measurements);
            IotDBMeasureList.Add(deviceShortNameByStorageGroupName, measurements);
            OpcVariableNodeDic.Add(deviceShortNameByStorageGroupName, referencesList);//NodeList

            //配置订阅的节点
            foreach (var n in referencesSubscriptionList)
            {
                opcUaClient.AddSubscription($"{deviceShortNameByStorageGroupName}|{n.DisplayName.Text}", n.NodeId.ToString(), SubscripCallBack);
            }


        }
        private bool ConnectIotDB()
        {


            //if (_iotclient != null&& _iotclient.IsOpen)
            //    return false;
#if false
            Dictionary<string, string> pairs = new Dictionary<string, string>();
            IotDBUrl.Split(';', StringSplitOptions.RemoveEmptyEntries).ForEach(f =>
            {
                var kv = f.Split('=');
                pairs.TryAdd(key: kv[0], value: kv[1]);
            });

            Console.WriteLine($"**********IotDBUrl属性，值：{IotDBUrl}");

            string host = pairs.GetValueOrDefault("Server") ?? "127.0.0.1";
            int port = int.Parse(pairs.GetValueOrDefault("Port") ?? "6667");
            string username = pairs.GetValueOrDefault("User") ?? "root";
            string password = pairs.GetValueOrDefault("Password") ?? "root";
            int fetchSize = int.Parse(pairs.GetValueOrDefault("fetchSize") ?? "1800");
            bool enableRpcCompression = bool.Parse(pairs.GetValueOrDefault("enableRpcCompression") ?? "false");
            int? poolSize = pairs.GetValueOrDefault("poolSize") != null ? int.Parse(pairs.GetValueOrDefault("poolSize")) : null;
            
#else

            string url = IotDBUrl;
            //url = iotdb://root:root@127.0.0.1:6667/?appName=iTSDB&fetchSize=1800
            var match_host = new Regex(@"@((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3}:").Match(url);
            var match_port = new Regex(@":(\d{1,5})/?").Match(url);
            var match_user = new Regex(@"iotdb://(\w+):").Match(url);
            var match_pwd = new Regex(@":(\w+\S+){1}(@)").Match(url);

            var host = match_host.Success ? match_host.Value[1..^1] : "127.0.0.1";
            var port = match_port.Success ? int.Parse(match_port.Value[1..].Replace("/", string.Empty)) : 6667;
            var username = match_user.Success ? match_user.Value[8..^1] : "root";
            var password = match_pwd.Success ? match_pwd.Value[1..^1] : "root";

            Dictionary<string, string> pairs = new Dictionary<string, string>();
            var kvParaStrIndex= IotDBUrl.IndexOf('?');
            if (kvParaStrIndex < 0)
                kvParaStrIndex = 0;
            IotDBUrl.Substring(kvParaStrIndex).Split('&', StringSplitOptions.RemoveEmptyEntries).ForEach(f =>
            {
                var kv = f.Split('=');
                pairs.TryAdd(key: kv[0], value: kv[1]);
            });

            int fetchSize = int.Parse(pairs.GetValueOrDefault("fetchSize") ?? "1800");
            bool enableRpcCompression = bool.Parse(pairs.GetValueOrDefault("enableRpcCompression") ?? "false");
            int? poolSize = pairs.GetValueOrDefault("poolSize") != null ? int.Parse(pairs.GetValueOrDefault("poolSize")) : null;

#endif
            _iotclient = new Salvini.IoTDB.Session(host, port, username, password, fetchSize, poolSize, enableRpcCompression);
            if (!_iotclient.CheckDataBaseOpen())
                return false;

            using var query = _iotclient.ExecuteQueryStatementAsync($"show storage group root.{StorageGroupName}");//判断存储组是否已经存在
            if (query.Result.HasNext())
            {
                //存储组已经存在，无需处理
            }
            else
            {
                _iotclient.CreateStorageGroup($"root.{StorageGroupName}");
            }
            return true;
        }

        public bool Connect()
        {
            try
            {
                if (_iotclient != null && _iotclient.IsOpen)
                    return true;


                Console.WriteLine($"-------------------IotDBUrl属性，值：{IotDBUrl}，TopNodeId_1：{TopNodeId_1}");
                //连接iotdb；
                ConnectIotDB();
                if (_iotclient == null)
                    return false;
                if (!_iotclient.IsOpen)
                    return false;

                OpcVariableNodeDic.Clear();
                IotDBMeasureList.Clear();
                LoadExcelVarData();
                opcUaClient = new OpcUaClientHelper() { OpcUaName = "CollectOPCUaDataClient" };

                var isok = opcUaClient.ConnectServer(Uri).Wait(Timeout);
                if (isok)
                {
                    //—,特殊字符：root.rczz.973工单接收.灌装B工位—压盖权限，待处理

                    opcUaClient.RemoveAllSubscription();//清除订阅

                    if(!string.IsNullOrEmpty(NodeIdFile)&&File.Exists(NodeIdFile))
                    {
                        //通过文本文件的方式获取
                        string[] nodeidStr= File.ReadAllLines(NodeIdFile, Encoding.UTF8);
                        var deviceShortNameByStorageGroupName = string.Format("{0}.{1}", StorageGroupName, NodeIdParentName);

                        List<ReferenceDescription> NodeList = new List<ReferenceDescription>();
                        for (int i = 0; i < nodeidStr.Length; i++)
                        {
                            string[] txt= nodeidStr[i].Split('\t');
                            if (txt.Length !=5)
                            {
                                continue;
                            }
                            ReferenceDescription referenceDescription = new ReferenceDescription() {
                                NodeId = new ExpandedNodeId(string.Format("{0}{1}", NodeIdNsPrefix, txt[4])),
                                BrowseName= txt[0],
                                DisplayName= txt[0],
                            };
                            NodeList.Add(referenceDescription);
                        }
                        InitOpcDriverNodeList(NodeList, deviceShortNameByStorageGroupName);
                    }
                    else
                    {
                        InitNodeList(this.TopNodeId_1.ReplaceNodeIdStr());
                    }



                    //if (IsSubscription)
                    //{
                    //    foreach (var item in OpcVariableNodeDic)
                    //    {
                    //        //item.Key
                    //        foreach (var n in item.Value)
                    //        {
                    //            opcUaClient.AddSubscription($"{item.Key}|{n.DisplayName.Text}", n.NodeId.ToString(), SubscripCallBack);
                    //        }

                    //    }
                    //    //opcUaClient.AddSubscription()
                    //}



                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception Message:{ex.Message}\n{ex.StackTrace}");
                return false;
            }
            return IsConnected;
        }

        public bool Close()
        {
            try
            {
                opcUaClient?.Disconnect();
                return !IsConnected;
                return true;
            }
            catch (Exception)
            {

                return false;
            }
        }

        public void Dispose()
        {
            try
            {
                opcUaClient = null;
            }
            catch (Exception)
            {

            }
        }



        [Method("读OPCUa", description: "读OPCUa节点")]
        public DriverReturnValueModel ReadNode(DriverAddressIoArgModel ioarg)
        {
            var ret = new DriverReturnValueModel { StatusType = VaribaleStatusTypeEnum.Good };

            if (IsConnected)
            {
                try
                {
                    var Tag = ioarg.Address.Replace("&quot;", "\"");
                    var dataValue = opcUaClient.ReadNode(new NodeId(Tag));

                    if (DataValue.IsGood(dataValue))
                        ret.Value = dataValue.Value;
                }
                catch (Exception ex)
                {
                    ret.StatusType = VaribaleStatusTypeEnum.Bad;
                    ret.Message = $"读取失败,{ex.Message}";
                }
            }
            else
            {
                ret.StatusType = VaribaleStatusTypeEnum.Bad;
                ret.Message = "连接失败";
            }
            return ret;
        }

        [Method("测试方法", description: "测试方法，返回当前时间")]
        public DriverReturnValueModel Read(DriverAddressIoArgModel ioarg)
        {
            var ret = new DriverReturnValueModel { StatusType = VaribaleStatusTypeEnum.Good };

            if (IsConnected)
                ret.Value = DateTime.Now;
            else
            {
                ret.StatusType = VaribaleStatusTypeEnum.Bad;
                ret.Message = "连接失败";
            }
            return ret;
        }

        private string GetOPCuaExtensionObjectValut(ExtensionObject v)
        {
            string str = string.Empty;
            switch (v.Encoding)
            {
                case ExtensionObjectEncoding.None:
                    break;
                case ExtensionObjectEncoding.Binary:
                    str = Encoding.UTF8.GetString((byte[])v.Body);
                    break;
                case ExtensionObjectEncoding.Xml:
                    break;
                case ExtensionObjectEncoding.EncodeableObject:
                    break;
                case ExtensionObjectEncoding.Json:
                    break;
                default:
                    break;
            }
            return str;
        }
        [Method("读OPCUa多个节点", description: "读OPCUa多个节点")]
        public DriverReturnValueModel ReadMultipleNode(DriverAddressIoArgModel ioarg)
        {
            var ret = new DriverReturnValueModel { StatusType = VaribaleStatusTypeEnum.Good };

            if (IsConnected)
            {
                try
                {
                    // ioarg.Address:  deviceShortName|tag
                    string[] tasInfor = ioarg.Address.Split("|");

                    var deviceShortNameByStorageGroupName = string.Format("{0}.{1}", StorageGroupName, tasInfor[0]);

                    string userTagID = string.Empty;
                    if (tasInfor.Length == 2)//存在 tagID 
                    {
                        userTagID = tasInfor[1];
                    }
                    List<ReferenceDescription> NodeList = null;

                    if (string.IsNullOrEmpty(userTagID))
                    {
                        if (OpcVariableNodeDic.ContainsKey(deviceShortNameByStorageGroupName))
                            NodeList = OpcVariableNodeDic[deviceShortNameByStorageGroupName];
                    }
                    else
                    {
                        NodeList = opcUaClient.BrowseNodeReference2(userTagID).Where(item =>
                        {
                            return (item.NodeClass == NodeClass.Variable);
                        }).ToList();
                    }
                    if (NodeList == null)
                    {
                        ret.StatusType = VaribaleStatusTypeEnum.Bad;
                        ret.Value = $"读取失败,设备信息与预置信息不匹配";
                        return ret;
                    }

                    var nodeIDList = NodeList.Select(item =>
                    {
                        return new NodeId(item.NodeId.Identifier, item.NodeId.NamespaceIndex);
                    }).ToArray();

                    if (nodeIDList.Length > 0)
                    {

                        int plcDB_MaxRows = PLCPointMaxCount;
                        int currentPage = 0;
                        int totlePage = (int)Math.Ceiling(((decimal)nodeIDList.Length / (decimal)plcDB_MaxRows));
                        SaveIotDBStatus saveIotRet = SaveIotDBStatus.初始状态;
                        while (currentPage < totlePage)
                        {
                            var tempData = nodeIDList.Skip(currentPage * plcDB_MaxRows).Take(plcDB_MaxRows).ToList();
                            var tempNodeList = NodeList.Skip(currentPage * plcDB_MaxRows).Take(plcDB_MaxRows).ToList();

                            var ts = DateTime.Now;
                            var opcData = opcUaClient.ReadNodes(tempData.ToArray());
                            var tempRes = SaveNodeDataValue2IotDb(deviceShortNameByStorageGroupName,
                                                                    ts, opcData, tempNodeList).Result;

                            if (saveIotRet == SaveIotDBStatus.初始状态)
                            {
                                saveIotRet = tempRes;
                            }
                            else if (saveIotRet != tempRes)
                            {
                                saveIotRet = SaveIotDBStatus.部分数据保存失败;
                            }

                            //Console.WriteLine($"读取第{currentPage}页,共计{totlePage},结果:{tempRes.ToString()}");
                            currentPage++;
                        }


                        ret.StatusType = saveIotRet == SaveIotDBStatus.成功? VaribaleStatusTypeEnum.Good: VaribaleStatusTypeEnum.MethodError;
                        ret.Value = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss}数据写入完成:{saveIotRet}，共计{nodeIDList.Length}个测点数据";
                    }
                }
                catch (Exception ex)
                {
                    ret.StatusType = VaribaleStatusTypeEnum.Bad;
                    ret.Value = $"读取失败,{ex.Message}";
                }
            }
            else
            {
                ret.StatusType = VaribaleStatusTypeEnum.Bad;
                ret.Value = $"OPC设备连接失败，url:{Uri}";
            }
            return ret;
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="deviceShortNameByStorageGroupName"></param>
        /// <param name="ts"></param>
        /// <param name="opcData"></param>
        /// <param name="NodeList"></param>
        /// <returns>0:写入成功，-1：写入失败,-2无可写入的数据</returns>
        private async Task<SaveIotDBStatus> SaveNodeDataValue2IotDb(string deviceShortNameByStorageGroupName, DateTime ts, List<DataValue> opcData, List<ReferenceDescription> NodeList)
        {

            try
            {

                List<(string Tag, object Value)> data = new List<(string Tag, object Value)>();
                for (int i = 0; i < opcData.Count; i++)
                {
                    var nodeData = opcData[i];
                    var nodeKey = NodeList[i];

                    if (DataValue.IsGood(nodeData))
                    {
                        string tagName = nodeKey.DisplayName.Text;
                        var v = nodeData.Value;
                        if (nodeData.Value == null)
                        {
                            v = "null";
                        }
                        var v_Type = v.GetType();
                        if (v_Type == typeof(UInt16))
                        {
                            Int32 tempV = (UInt16)v;
                            data.Add((tagName, tempV));
                        }
                        else if (v_Type == typeof(Byte))
                        {
                            Int32 tempV = (Byte)v;
                            data.Add((tagName, tempV));
                        }
                        else if (v_Type == typeof(UInt32) || v_Type == typeof(UInt64))
                        {
                            long tempV = 0;
                            if (long.TryParse(v.ToString(), out tempV))
                                data.Add((tagName, tempV));
                            else
                            {
                                Console.WriteLine($"{v_Type}:{v},无法转换为long类型.");
                            }
                        }
                        else if (v_Type == typeof(SByte))
                        {
                            Int32 tempV = (SByte)v;
                            data.Add((tagName, tempV));
                        }
                        else if (v_Type == typeof(Int16))
                        {
                            Int32 tempV = (Int16)v;
                            data.Add((tagName, tempV));
                        }
                        else if (v_Type == typeof(Int16[]))
                        {
                            Int16[] tempV = (Int16[])v;
                            StringBuilder sb = new StringBuilder();
                            for (int tempIndex = 0; tempIndex < tempV.Length; tempIndex++)
                            {
                                sb.Append(tempV[tempIndex].ToString());
                            }
                            data.Add((tagName, sb.ToString()));
                        }
                        else if (v_Type == typeof(UInt16[]))
                        {
                            UInt16[] tempV = (UInt16[])v;
                            StringBuilder sb = new StringBuilder();
                            for (int tempIndex = 0; tempIndex < tempV.Length; tempIndex++)
                            {
                                sb.Append(tempV[tempIndex].ToString());
                            }
                            data.Add((tagName, sb.ToString()));
                        }
                        else if (v_Type == typeof(System.DateTime))
                        {
                            DateTime tempV = (DateTime)v; //
                            data.Add((tagName, tempV.UTC_MS()));
                        }
                        else if (v_Type == typeof(System.UInt64))
                        {
                            System.UInt64 tempV = (System.UInt64)v;
                            data.Add((tagName, tempV));
                        }
                        else if (v_Type == typeof(System.Xml.XmlElement))
                        {
                            System.Xml.XmlElement obj = (System.Xml.XmlElement)v;
                            data.Add((tagName, obj.Value));
                        }
                        else if (v_Type == typeof(byte[]))
                        {
                            byte[] obj = (byte[])v;
                            data.Add((tagName, System.Text.Encoding.UTF8.GetString(obj)));
                        }
                        else if (v_Type == typeof(Guid))
                        {
                            Guid obj = (Guid)v;
                            data.Add((tagName, v.ToString()));
                        }
                        else if (v_Type == typeof(Opc.Ua.ExtensionObject))
                        {
                            ExtensionObject obj = (ExtensionObject)v;
                            data.Add((tagName, GetOPCuaExtensionObjectValut(obj)));
                        }
                        else if (v_Type == typeof(Opc.Ua.LocalizedText))
                        {
                            Opc.Ua.LocalizedText obj = (Opc.Ua.LocalizedText)v;
                            data.Add((tagName, obj.Text));
                        }
                        else if (v_Type == typeof(Opc.Ua.Uuid))
                        {
                            Opc.Ua.Uuid obj = (Opc.Ua.Uuid)v;
                            data.Add((tagName, obj.GuidString));
                        }
                        else if (v_Type == typeof(Opc.Ua.QualifiedName))
                        {
                            Opc.Ua.QualifiedName obj = (Opc.Ua.QualifiedName)v;
                            data.Add((tagName, obj.Name));
                        }//
                        else if (v_Type == typeof(Opc.Ua.StatusCode))
                        {
                            Opc.Ua.StatusCode obj = (Opc.Ua.StatusCode)v;
                            data.Add((tagName, obj.Code.ToString()));
                        }//
                        else if (v_Type == typeof(Opc.Ua.ExpandedNodeId))
                        {
                            Opc.Ua.ExpandedNodeId obj = (Opc.Ua.ExpandedNodeId)v;

                            switch (obj.IdType)
                            {
                                case IdType.Numeric:
                                    var v1 = (long.Parse(obj.Identifier.ToString()));
                                    data.Add((tagName, v1));
                                    break;
                                case IdType.String:
                                    var v2 = ((string)(obj.Identifier));
                                    data.Add((tagName, v2));
                                    break;
                                case IdType.Guid:
                                    var v3 = ((Guid)(obj.Identifier)).ToString();
                                    data.Add((tagName, v3));
                                    break;
                                case IdType.Opaque:
                                    var v4 = System.Text.Encoding.UTF8.GetString((byte[])(obj.Identifier));
                                    data.Add((tagName, v4));
                                    break;
                                default:
                                    break;
                            }

                        }//Opc.Ua.ExpandedNodeId
                        else if (v_Type == typeof(Opc.Ua.NodeId))
                        {
                            Opc.Ua.NodeId obj = (Opc.Ua.NodeId)v;

                            switch (obj.IdType)
                            {
                                case IdType.Numeric:
                                    var v1 = (long.Parse(obj.Identifier.ToString()));
                                    data.Add((tagName, v1));
                                    break;
                                case IdType.String:
                                    var v2 = ((string)(obj.Identifier));
                                    data.Add((tagName, v2));
                                    break;
                                case IdType.Guid:
                                    var v3 = ((Guid)(obj.Identifier)).ToString();
                                    data.Add((tagName, v3));
                                    break;
                                case IdType.Opaque:
                                    var v4 = System.Text.Encoding.UTF8.GetString((byte[])(obj.Identifier));
                                    data.Add((tagName, v4));
                                    break;
                                default:
                                    break;
                            }
                        }
                        else
                            data.Add((tagName, v));


                        //Opc.Ua.QualifiedName
                    }
                    else
                        Console.WriteLine("not good");
                }
                if (data.Count > 0)
                {
                    //iotdb 貌似不支持一次写入太多数据
                    int iotDB_MaxRows = PLCPointMaxCount;
                    int currentPage = 0;
                    int totlePage =(int)Math.Ceiling(((decimal)data.Count / (decimal)iotDB_MaxRows));
                    SaveIotDBStatus ret= SaveIotDBStatus.初始状态;
                    while (currentPage < totlePage)
                    {
                       var tempData= data.Skip(currentPage* iotDB_MaxRows).Take(iotDB_MaxRows).ToList();
                       var tempRes=  (SaveIotDBStatus)await _iotclient.BulkWriteAsync(deviceShortNameByStorageGroupName, ts, tempData);
                        if(ret == SaveIotDBStatus.初始状态 )
                        {
                            ret = tempRes;
                        }else if(ret != tempRes)
                        {
                            ret = SaveIotDBStatus.部分数据保存失败;
                        }

                        if(tempRes!=  SaveIotDBStatus.成功)
                        {

                        }
                        currentPage++;
                    }

                    return ret;
                }
                else
                    return SaveIotDBStatus.无数据;
            }
            catch (Exception ex)
            {

                throw;
            }

        }
        public async Task<RpcResponse> WriteAsync(string RequestId, string Method, DriverAddressIoArgModel Ioarg)
        {
            RpcResponse rpcResponse = new() { IsSuccess = false, Description = "设备驱动内未实现写入功能" };
            return rpcResponse;
        }
    }
}