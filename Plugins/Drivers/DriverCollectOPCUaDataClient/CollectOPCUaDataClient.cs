using DriverCollectOPCUaDataClient.IotDbExt;
using Opc.Ua;
using OpcUaHelper;
using PluginInterface;
using Silkier.Extensions;
using System.Text;

namespace DriverCollectOPCUaDataClient
{

    [DriverSupported("OPC UA && IotDB")]
    [DriverInfoAttribute("CollectOPCUaDataClient", "V0.1.0", "Copyright ccliushou 2022-7-8")]
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



        [ConfigParameter("IotDB 地址")]
        public string IotDBUrl { get; set; } = @"iotdb://root:root@127.0.0.1:6667/appName=testapp";

        [ConfigParameter("IotDb存储组名称STORAGE group")]
        public string StorageGroupName { get; set; } = "rczz";

        [ConfigParameter("第一组NodeId")]
        public string TopNodeId_1 { get; set; } = "数据1|ns=2;s=实时模拟";
        [ConfigParameter("第二组NodeId")]
        public string TopNodeId_2 { get; set; } = "数据2|ns=2;s=实时模拟";
        [ConfigParameter("第三组NodeId")]
        public string TopNodeId_3 { get; set; } = "数据3|ns=2;s=实时模拟";
        [ConfigParameter("第四组NodeId")]
        public string TopNodeId_4 { get; set; } = "数据4|ns=2;s=实时模拟";
        [ConfigParameter("第五组NodeId")]
        public string TopNodeId_5 { get; set; } = "数据5|ns=2;s=实时模拟";
        [ConfigParameter("第六组NodeId")]
        public string TopNodeId_6 { get; set; } = "数据6|ns=2;s=实时模拟";


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

            //连接iotdb；
            ConnectIotDB();
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
            ReferenceDescription[] NodeList = opcUaClient.BrowseNodeReference2(tag);
            var nodeDescriptionList = NodeList.Where(item =>
            {
                return (item.NodeClass == NodeClass.Variable);
            }).ToList();
            return nodeDescriptionList;
        }
        private void InitNodeList(string TopTag)
        {
            string[] tasInfor = TopTag.Split("|");
            if (tasInfor.Length != 2)
                return;

            var NodeList = GetBrowseVariableNodeList(tasInfor[1]);
            var deviceShortNameByStorageGroupName = string.Format("{0}.{1}", StorageGroupName, tasInfor[0]);
            var iotFulldeviceName = string.Format("root.{0}", deviceShortNameByStorageGroupName);
            List<(string Tag, Type Type, string Desc, string Unit, double? Downlimit, double? Uplimit)> measurements = new List<(string Tag, Type Type, string Desc, string Unit, double? Downlimit, double? Uplimit)>();
            NodeList.ForEach(node =>
             {
                 var dataValue = opcUaClient.ReadNode(new NodeId(node.NodeId.ToString()));//读取一次
                 if (DataValue.IsGood(dataValue))
                 {
                     //
                     //measurements.Add(("Latitude", "double", "gps Latitude", null, null, null));
                     measurements.Add((node.DisplayName.Text, dataValue.Value.GetType(), node.BrowseName.ToString(), null, null, null));
                 }
             });

            //
            _iotclient.InitializeAsync(deviceShortNameByStorageGroupName, measurements);

            IotDBMeasureList.Add(deviceShortNameByStorageGroupName, measurements);
            OpcVariableNodeDic.Add(deviceShortNameByStorageGroupName, NodeList);

        }
        private void ConnectIotDB()
        {
            if (_iotclient != null)
                return;

            Dictionary<string, string> pairs = new Dictionary<string, string>();
            IotDBUrl.Split(';', StringSplitOptions.RemoveEmptyEntries).ForEach(f =>
            {
                var kv = f.Split('=');
                pairs.TryAdd(key: kv[0], value: kv[1]);
            });
            string host = pairs.GetValueOrDefault("Server") ?? "127.0.0.1";
            int port = int.Parse(pairs.GetValueOrDefault("Port") ?? "6667");
            string username = pairs.GetValueOrDefault("User") ?? "root";
            string password = pairs.GetValueOrDefault("Password") ?? "root";
            int fetchSize = int.Parse(pairs.GetValueOrDefault("fetchSize") ?? "1800");
            bool enableRpcCompression = bool.Parse(pairs.GetValueOrDefault("enableRpcCompression") ?? "false");
            int? poolSize = pairs.GetValueOrDefault("poolSize") != null ? int.Parse(pairs.GetValueOrDefault("poolSize")) : null;
            _iotclient = new Salvini.IoTDB.Session(host, port, username, password, fetchSize, poolSize, enableRpcCompression);
            _iotclient.CheckDataBaseOpen();

            using var query = _iotclient.ExecuteQueryStatementAsync($"show storage group root.{StorageGroupName}");//判断存储组是否已经存在
            if (query.Result.HasNext())
            {
                //存储组已经存在，无需处理
            }
            else
            {
                _iotclient.CreateStorageGroup($"root.{StorageGroupName}");
            }

        }

        public bool Connect()
        {
            try
            {
                OpcVariableNodeDic.Clear();
                IotDBMeasureList.Clear();
                opcUaClient = new OpcUaClientHelper() { OpcUaName = "CollectOPCUaDataClient" };
                var isok = opcUaClient.ConnectServer(Uri).Wait(Timeout);
                if (isok)
                {

                    //var Tag = ioarg.Address.Replace("&quot;", "\"");
                    InitNodeList(this.TopNodeId_1.ReplaceNodeIdStr());
                    InitNodeList(this.TopNodeId_2.ReplaceNodeIdStr());
                    InitNodeList(this.TopNodeId_3.ReplaceNodeIdStr());
                    InitNodeList(this.TopNodeId_4.ReplaceNodeIdStr());
                    InitNodeList(this.TopNodeId_5.ReplaceNodeIdStr());
                    InitNodeList(this.TopNodeId_6.ReplaceNodeIdStr());
                }
            }
            catch (Exception)
            {
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
                        if(OpcVariableNodeDic.ContainsKey(deviceShortNameByStorageGroupName))
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
                        return new NodeId(item.NodeId.Identifier.ToString(), item.NodeId.NamespaceIndex);
                    }).ToArray();

                    if (nodeIDList.Length > 0)
                    {

                        var ts =DateTime.Now;
                        List <(string Tag, object Value)> data = new List<(string Tag, object Value)>(); 
                        //Dictionary<string, dynamic> dataValues = new Dictionary<string, dynamic>();
                        var opcData = opcUaClient.ReadNodes(nodeIDList);
                        for (int i = 0; i < opcData.Count; i++)
                        {
                            var nodeData = opcData[i];
                            var nodeKey = NodeList[i];

                            if (DataValue.IsGood(nodeData))
                            {
                                string tagName = nodeKey.DisplayName.Text;
                                var v = nodeData.Value;
                                var v_Type = v.GetType(); 
                                if (v_Type == typeof(UInt16))
                                {
                                    Int32 tempV =(UInt16)v;
                                    data.Add((tagName, tempV));
                                }
                                else if (v_Type == typeof(Int16))
                                {
                                    Int32 tempV = (Int16)v;
                                    data.Add((tagName, tempV));
                                }
                                else if(v_Type == typeof(Opc.Ua.ExtensionObject))
                                {
                                    ExtensionObject obj= (ExtensionObject)v;
                                    data.Add((tagName, GetOPCuaExtensionObjectValut(obj)));
                                }
                                else
                                    data.Add((tagName, v));

                            }
                        }
                        if(data.Count>0)
                        {
                            _iotclient.CheckDataBaseOpen();
                            _iotclient.BulkWriteAsync(deviceShortNameByStorageGroupName, ts, data);
                        }

                        ret.StatusType = VaribaleStatusTypeEnum.Good;
                        ret.Value = $"{ts:yyyy-MM-dd HH:mm:ss}数据写入成功，{data.Count}个测点数据";
                        //if (dataValues.Count > 0)
                        //    ret.Value = dataValues;
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
                ret.Message = "连接失败";
            }
            return ret;
        }


        public async Task<RpcResponse> WriteAsync(string RequestId, string Method, DriverAddressIoArgModel Ioarg)
        {
            RpcResponse rpcResponse = new() { IsSuccess = false, Description = "设备驱动内未实现写入功能" };
            return rpcResponse;
        }
    }
}