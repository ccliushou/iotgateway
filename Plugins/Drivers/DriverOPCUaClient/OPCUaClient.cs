using PluginInterface;
using System;
using System.Collections;
using System.IO;
using Opc.Ua;
using Opc.Ua.Client;
using System.Collections.Generic;
using System.Threading.Tasks;
using Opc.Ua.Configuration;
using OpcUaHelper;
using System.Globalization;

namespace DriverOPCUaClient
{
    [DriverSupported("OPC UA")]
    [DriverInfoAttribute("OPCUaClient", "V1.0.0", "Copyright IoTGateway© 2021-12-19")]
    public class OPCUaClient : IDriver
    {
        OpcUaClientHelper opcUaClient = null;
        #region 配置参数

        [ConfigParameter("设备Id")]
        public Guid DeviceId { get; set; }

        [ConfigParameter("uri")]
        public string Uri { get; set; } = "opc.tcp://localhost:62541/Quickstarts/ReferenceServer";

        [ConfigParameter("超时时间ms")]
        public int Timeout { get; set; } = 3000;

        [ConfigParameter("最小通讯周期ms")]
        public uint MinPeriod { get; set; } = 3000;



        [ConfigParameter("TopNodeId")]
        public string TopNodeId { get; set; } = "ns=2;s=实时模拟";


        #endregion

        public OPCUaClient(Guid deviceId)
        {
            DeviceId = deviceId;

        }


        public bool IsConnected
        {
            get
            {

                return opcUaClient != null && opcUaClient.Connected;
            }
        }

        public bool Connect()
        {
            try
            {
                opcUaClient = new OpcUaClientHelper();
                opcUaClient.ConnectServer(Uri).Wait(Timeout);
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
                    var Tag =  ioarg.Address.Replace("&quot;", "\"");
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



        [Method("浏览opc节点，查询可读取的节点数量", description: "浏览opc节点，查询可读取的节点数量")]
        public DriverReturnValueModel BrowseNode(DriverAddressIoArgModel ioarg)
        {
            var ret = new DriverReturnValueModel { StatusType = VaribaleStatusTypeEnum.Good };

            if (IsConnected)
            {
                try
                {
                    
                    if (opcUaClient == null)
                        return ret;

                    string userTag = ioarg.Address;

                    ReferenceDescription[] NodeList = opcUaClient.BrowseNodeReference(userTag);
                    var str = string.Join('\n', NodeList.Select(x => x.NodeId.ToString()).ToArray());
                    Console.WriteLine("*************************\n" + str);
                    for (int i = 0; i < NodeList.Length; i++)
                    {
                        var Node = NodeList[i];
                        var childTag = Node.NodeId.ToString();
                        if (opcUaClient == null)
                            return ret;
                        var childNodeList= opcUaClient.BrowseNodeReference(childTag);
                        if(childNodeList!=null && childNodeList.Length>0)
                        {
                            var str2 = string.Join('\n', childNodeList.Select(x => x.NodeId.ToString()).ToArray());
                            Console.WriteLine(childTag+"--------------------\n" + str2);
                        }
                    } 
                    ret.Value = NodeList.Count(item => { return item.NodeClass == NodeClass.Variable; });
                   
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




        [Method("浏览opc节点，查询可读取的节点数量", description: "浏览opc节点，查询可读取的节点数量")]
        public DriverReturnValueModel BrowseNode2(DriverAddressIoArgModel ioarg)
        {
            var ret = new DriverReturnValueModel { StatusType = VaribaleStatusTypeEnum.Good };

            if (IsConnected)
            {
                try
                {

                    if (opcUaClient == null)
                        return ret;

                    string userTag = ioarg.Address;

                    ReferenceDescription[] NodeList = opcUaClient.BrowseNodeReference2(userTag);
                    var str = string.Join('\n', NodeList.Select(x => x.NodeId.ToString()).ToArray());
                    Console.WriteLine("*************************\n" + str);
                    for (int i = 0; i < NodeList.Length; i++)
                    {
                        var Node = NodeList[i];
                        var childTag = Node.NodeId.ToString();
                        if (opcUaClient == null)
                            return ret;
                        var childNodeList = opcUaClient.BrowseNodeReference2(childTag);
                        if (childNodeList != null && childNodeList.Length > 0)
                        {
                            var str2 = string.Join('\n', childNodeList.Select(x => x.NodeId.ToString()).ToArray());
                            Console.WriteLine(childTag + "--------------------\n" + str2);
                        }
                    }
                    ret.Value = NodeList.Count(item => { return item.NodeClass == NodeClass.Variable; });

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




        [Method("读OPCUa多个节点", description: "读OPCUa多个节点")]
        public DriverReturnValueModel ReadMultipleNode(DriverAddressIoArgModel ioarg)
        {
            var ret = new DriverReturnValueModel { StatusType = VaribaleStatusTypeEnum.Good };

            if (IsConnected)
            {
                try
                {
                    string[] strArray = ioarg.Address.Split("|");
                    string tag = strArray[0];

                    string replaceMeasuStr=string.Empty; 
                    if (strArray.Length==2)//存在要替换的 测点名称
                    {
                        replaceMeasuStr=strArray[1];
                    }
                    ReferenceDescription[] NodeList = opcUaClient.BrowseNodeReference2(tag);
                    var nodeIDList = NodeList.Where(item =>
                        {
                            return (item.NodeClass == NodeClass.Variable);
                        }).Select(item =>
                        {
                            return new NodeId(item.NodeId.Identifier.ToString(), item.NodeId.NamespaceIndex);
                        }).ToArray();
                    if(nodeIDList.Length > 0)
                    {
                        Dictionary<string, dynamic> dataValues = new Dictionary<string, dynamic>();
                        var opcData = opcUaClient.ReadNodes(nodeIDList);
                        for (int i = 0; i < opcData.Count; i++)
                        {
                            var nodeData = opcData[i];
                            var nodeKey = nodeIDList[i];
                            if (DataValue.IsGood(nodeData))
                            {
                                string keyStr = nodeKey.ToString();
                                int num2 = keyStr.IndexOf(';');
                                string tempKey = keyStr.Substring(num2 + 3);

                                tempKey = tempKey.Replace('#', ' ')
                                                .Replace("\"", "");

                                if (!string.IsNullOrEmpty(replaceMeasuStr))
                                    tempKey = tempKey.Replace(replaceMeasuStr, "");
                                dataValues.Add(string.Format("_{0}",tempKey), nodeData.Value);
                            }
                        }
                        //var dataValues = opcUaClient.ReadNodes(nodeIDList).Where(item =>
                        //{
                        //    return DataValue.IsGood(item);
                        //}).Select(item =>
                        //{
                        //    return item;
                        //}).ToList();

                        if (dataValues.Count > 0)
                            ret.Value = dataValues;
                    }
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


        public async Task<RpcResponse> WriteAsync(string RequestId, string Method, DriverAddressIoArgModel Ioarg)
        {
            RpcResponse rpcResponse = new() { IsSuccess = false, Description = "设备驱动内未实现写入功能" };
            return rpcResponse;
        }
    }
}
